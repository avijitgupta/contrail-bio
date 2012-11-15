/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Avijit Gupta (mailforavijit@gmail.com)

package contrail.correct;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.FileHelper;

import java.util.*;

/**
 * Quake is a tool that is used to correct fastq reads. This class deals with correction of non mate-pair reads.
 * The input to this class is an AVRO file which contains fastq records
 * Assumptions: 
 * 1 - Quake binary is available, and its path specified in the parameters. This is used 
 * later to load the flash binary from the location into Distributed Cache
 * We have access to a system temp directoy on each node; i.e the java command File.createTempFile succeeds.
 * The bitvector of kmers above the cutoff has been constructed and is available at a location 
 * specified in the input
 * Execution:
 * fastq records are written in blocks of blockSize onto the local temporary files of cluster 
 * nodes. Quake is executed via exec and the results are collected. Care must be taken to clean 
 * blocks of files when they are of no use.
 */ 
 
public class InvokeQuakeForSingles extends Stage{
  private static final Logger sLogger = Logger.getLogger(InvokeQuakeForSingles.class);
  public static int blockSize = 10000;

  public static class RunQuakeMapper extends AvroMapper<fastqrecord, fastqrecord>{	
    private String localOutFolderPath = null;
    private String quakeHome = null;
    private String tempWritableFolder = null;
    private String bithashPath = null;
    private ArrayList<String> fastqRecordList;
    private long K;
    private String blockFolder;
    private int count;
    private CorrectUtil correctUtil;
    private String jobName;
    private AvroCollector<fastqrecord> outputCollector;
    
    @Override
    public void configure(JobConf job){
      //Retrieving required parameters from the configuration
      InvokeQuakeForSingles stage = new InvokeQuakeForSingles();
      jobName = job.get("mapred.task.id");
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      tempWritableFolder = FileHelper.createLocalTempDir().getAbsolutePath();
      K = (Long)(definitions.get("K").parseJobConf(job));  
      correctUtil = new CorrectUtil();
      //Initialise empty array lists
      fastqRecordList= new ArrayList<String>();
      count = 0;
      outputCollector = null;
      //gets the dcache path of file named correct
      quakeHome = correctUtil.getDcachePath("correct", job);
      //gets the dcache path of the file named bithash
      bithashPath = correctUtil.getDcachePath("bithash", job);
      if(quakeHome.length() > 0){
        sLogger.info("Quake Home: " + quakeHome);  
      }
      else{
        sLogger.info("Error in reading binary from Dcache");
      }
      if(bithashPath.length() > 0){
          sLogger.info("Bithash Location: " + bithashPath);  
      }
      else{
        sLogger.info("Error in reading bithash from Dcache");
      }
          
  }
  
  public void map(fastqrecord fastqRecord, 
                  AvroCollector<fastqrecord> collector, Reporter reporter) throws IOException {
    if(outputCollector == null){
      outputCollector = collector;
    }
    count++;
    fastqRecordList.add(correctUtil.fastqRecordToString(fastqRecord));
    //Time to process one block
    if(count ==blockSize){
      runQuakeSingleOnInMemoryReads(collector);
      count = 0;
    }
  }
  
  /**
   * This method runs flash locally and collects the results.
   * @param output: The reference of the collector
   * @throws IOException
   */
  private void runQuakeSingleOnInMemoryReads(AvroCollector<fastqrecord> output)throws IOException {
    String filePathFq1;
    //gets the current timestamp in nanoseconds
    long time = System.nanoTime();
    //Converts the timestamp to a string. 
    String localTime;
    localTime = ""+time;
    // blockSize number of reads are written to a temporary folder - blockFolder
    // blockFolder is a combination of mapred taskid and timestamp to ensure uniqueness 
    // The input files are names <timestamp_1>.fq and <timestamp>_2.fq. Flash is executed
    // on these and the output file produced is out.extendedFrags.fastq in the same directory.
    // During cleanup, we can delete the blockFolder directly
    
    blockFolder = jobName+time;
    sLogger.info("block folder: " + blockFolder);
    //Create temp file names
    localOutFolderPath = new File(tempWritableFolder,blockFolder).getAbsolutePath();
    sLogger.info("local out folder: " + localOutFolderPath);
    File tempFile = new File(localOutFolderPath);
    if(!tempFile.exists()){
      tempFile.mkdir();
    }
    
    filePathFq1 = new File(localOutFolderPath,time + ".fq").getAbsolutePath();
    correctUtil.writeLocalFile(fastqRecordList,filePathFq1);  
    correctRunner(filePathFq1, localTime, quakeHome, localOutFolderPath, bithashPath);
    fastqRecordList.clear();
    String tempPath1 = filePathFq1.substring(0,filePathFq1.lastIndexOf('.'));
    String correctedFilePath1 = tempPath1 + ".cor.fq";
    sLogger.info("corrected path: " + correctedFilePath1);
    correctUtil.emitFastqFileToHDFS(new File(correctedFilePath1), output);
    //Clear temporary files
    tempFile = new File(localOutFolderPath);
    if(tempFile.exists()){
      FileUtils.deleteDirectory(tempFile);
    }
  }
 
  public void correctRunner(String fastqLocation, String localTime, String CorrectLocation, String QuakeLocalInputFolderPath, String bitHashLocation){
    String fastqListLocation = new File(QuakeLocalInputFolderPath, localTime+".txt").getAbsolutePath();
    correctUtil.writeStringToFile(fastqLocation, fastqListLocation);
    //Correction command
    String command=CorrectLocation+" -f "+ fastqListLocation + " -k " + K + " -b "+bitHashLocation;
    correctUtil.executeCommand(command);  
  } 
  
  /**
   * Writes out the remaining chunk of data which is a non multiple of blockSize
   */
  public void close() throws IOException{
    if(count > 0){
    	runQuakeSingleOnInMemoryReads(outputCollector);
    } 
   //delete the top level directory, and everything beneath
    File tempFile = new File(tempWritableFolder);
    if(tempFile.exists()){
        FileUtils.deleteDirectory(tempFile);
    }
  }
}
	
	/* creates the custom definitions that we need for this phase*/
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition quakeBiary = new ParameterDefinition("quake_binary", "any" +
            "Quake directory path", String.class, new String(""));
    ParameterDefinition bithashpath = new ParameterDefinition("bithashpath", "any" +
            "The path of bithash", String.class, new String(""));
    ParameterDefinition K = new ParameterDefinition("K", "any" +
    	    "Kmer length", Long.class, 0);
    for (ParameterDefinition def: new ParameterDefinition[] { quakeBiary, bithashpath, K}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public RunningJob runJob() throws Exception {
    JobConf conf = new JobConf(InvokeQuakeForSingles.class);
    conf.setJobName("Quake Singles invocation");
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    String quakePath = (String) stage_options.get("quake_binary");
    String bithashPath = (String) stage_options.get("bithashpath");
    //User wants to run quake
    if(quakePath.length()!= 0){
      DistributedCache.addCacheFile(new Path(quakePath).toUri(),conf);
    }
    else{
      throw new Exception("Please specify Quake path");
    }
    if(bithashPath.length()!= 0){
        DistributedCache.addCacheFile(new Path(bithashPath).toUri(),conf);
    }
    else{
        throw new Exception("Please specify bithash path");
    }
    //Sets the parameters in JobConf
    initializeJobConfiguration(conf);
    AvroJob.setMapperClass(conf, RunQuakeMapper.class);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    ///Input  
    fastqrecord read = new fastqrecord();
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setOutputSchema(conf, new fastqrecord().getSchema());
    //Map Only Job
    conf.setNumReduceTasks(0);
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }	    
    long starttime = System.currentTimeMillis();            
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;	
  }
	
  public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new InvokeQuakeForSingles(), args);
	System.exit(res);
  }
}
	

