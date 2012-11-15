package contrail.correct;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.avro.Schema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.FastqPreprocessorAvroCompressed;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;


import java.util.*;

import java.util.Date;

/*
 *This class works on the joined fastQ files that are present on the HDFS,
 *reads them and repartitions them into two fastQ files onto a local system.
 *Once these local files have been written, flash is invoked on these repartitioned 
 *files to form extended fragments which we use. This invocation of flash is done
 *within the close() method of the mapper.
 */
public class createPairedReadsForFlash extends Stage{

  /*parameters:
  * fastq1: An arraylist of records that belong to the 1st fastQ file
  * fastq2: An arraylist of records that belong to the 2nd fastQ file
  * fp1: file path 1 for mate 1. This is computed by appending timestamp within the folder that contains
  * input files for flash
  * fp2: file path 2 for mate 2. This is also computed in a manner similar to fp1
  */
  static void writeLocalFile(ArrayList<String> fastq1,ArrayList<String> fastq2,String fp1, String fp2){
    try{
    	  //Writing to the First File
		  FileWriter fstream = new FileWriter(fp1,true);
		  BufferedWriter out = new BufferedWriter(fstream);
		  for(int i=0;i<fastq1.size();i++)
		  {
		    out.write(fastq1.get(i)+"\n");
		  }
		  out.close();
		  fstream.close();
		  //Writing to the Second File
		  fstream = new FileWriter(fp2,true);
		  out = new BufferedWriter(fstream);
		  for(int i=0;i<fastq2.size();i++)
		  {
	   	    out.write(fastq2.get(i)+"\n");
		  }
		  out.close();
		  fstream.close();
		}
		catch (Exception e)	{
		  e.printStackTrace();
		}
	}
	
	public static class RunFlashMapper extends AvroMapper<joinedfqrecord, NullWritable>{	
	
	
	String filePathFq1,filePathFq2;
	String localTime;
	String FlashFinalOut=null;
	String FlashLocalOutFolderName = null;
	String FlashLocalOutFolderPath = null;
	String FlashLocalInputFolderPath = null;
	String FlashHome = null;
	String tempWritableFolder = null;
	String flashLocalInput =null;
	private ArrayList<String> temp_arraylist_1;
	private ArrayList<String> temp_arraylist_2;
	int count;
	
  @Override
  public void configure(JobConf job) 
  {
   	Calendar calendar = Calendar.getInstance();
    java.util.Date now = calendar.getTime();
    java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
    //gets the current timestamp in nanoseconds
    int time = currentTimestamp.getNanos();
    //Converts the timestamp to a string. 
    localTime = ""+time;
    //Retrieving required parameters from the configuration
    createPairedReadsForFlash stage = new createPairedReadsForFlash();
    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
    flashLocalInput = (String)(definitions.get("flashLocalInput").parseJobConf(job));
    FlashFinalOut = (String)(definitions.get("outputpath").parseJobConf(job));
   // tempWritableFolder = (String)(definitions.get("tempWritableFolder").parseJobConf(job));
    tempWritableFolder = (String)(definitions.get("tempWritableFolder").parseJobConf(job));
    FlashLocalOutFolderName = (String)(definitions.get("flashLocalOutput").parseJobConf(job));
    FlashHome = (String)(definitions.get("flashHome").parseJobConf(job));
    /*
     * We maintain only one writable folder that a user is supposed to provide on every cluster
     * node. We call this tempWritableFolder. Within this global writable folder, 
     * we require specific folders, such as FlashLocalOutFolderName, which store
     * temporary flash data. The temporary files written to local disk on every cluster node
     * when a repartition is done are named by their timestamp_1/2.fq. We assume that we wont have
     * collisions in this naming.
     */
        
      FlashLocalOutFolderPath =  tempWritableFolder+"/"+FlashLocalOutFolderName;
        
      //Create temporary folder within tempWritableFolder
      FlashLocalInputFolderPath = tempWritableFolder+"/"+flashLocalInput;
      File dir = new File(FlashLocalInputFolderPath);
      if(!dir.exists())
      	dir.mkdir();

      //Create temp file names
       
      filePathFq1 = tempWritableFolder+"/"+flashLocalInput+"/"+time+"_1.fq";
      filePathFq2 = tempWritableFolder+"/"+flashLocalInput+"/"+time+"_2.fq";
	  //Initialise empty array lists
	  temp_arraylist_1= new ArrayList<String>();
	  temp_arraylist_2= new ArrayList<String>();
	  count = 0;
  }
   
  public void map(joinedfqrecord joined_record, 
	            AvroCollector<NullWritable> output, Reporter reporter) throws IOException {
	   
    String seqId = joined_record.getId1().toString();
    String seq1 = joined_record.getRead1().toString();
    String qval1 = joined_record.getQvalue1().toString();
    String seqId2 = joined_record.getId2().toString();
    String seq2 = joined_record.getRead2().toString();
    String qval2 = joined_record.getQvalue2().toString();
    count ++;
    temp_arraylist_1.add(seqId+"/1\n"+seq1+"\n"+"+\n"+qval1);
    temp_arraylist_2.add(seqId2+"/2\n"+seq2+"\n"+"+\n"+qval2);
    if(count ==10000)
    {
      createPairedReadsForFlash.writeLocalFile(temp_arraylist_1,temp_arraylist_2,filePathFq1,filePathFq2);  
	  temp_arraylist_1.clear();
	  temp_arraylist_2.clear();
	  count =0;
    }	
    output.collect(NullWritable.get());
  }
	 	
  public void close() throws IOException{
    if(count > 0){
      createPairedReadsForFlash.writeLocalFile(temp_arraylist_1,temp_arraylist_2,filePathFq1,filePathFq2);
      count = 0;
    }
    File dir = new File(FlashLocalOutFolderPath);
    if(!dir.exists())
	  dir.mkdir();        
    runFlash.flashRunner(filePathFq1, filePathFq2, localTime, FlashFinalOut, FlashLocalOutFolderPath, FlashHome);
    //Cleaning Up local Files
    File fp = new File(filePathFq1);
    if(fp.exists())fp.delete();
    fp = new File(filePathFq2);
    if(fp.exists())fp.delete();
    }
  }
	
	/* creates the custom definitions that we need for this phase*/
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = 
	        new HashMap<String, ParameterDefinition>();
	defs.putAll(super.createParameterDefinitions());
	ParameterDefinition flashLocalInput = new ParameterDefinition("flashLocalInput", "any" +
	"The name of the folder that is used to store splitted files which serve as " +
	"input for flash. We create this automatically within the tempFolder", String.class, new String(""));
	ParameterDefinition tempWritableFolder = new ParameterDefinition("tempWritableFolder", "any" +
	"The folder that is writable provided on every node in the" +
	"cluster with write permissions", String.class, new String(""));
	ParameterDefinition flashHome = new ParameterDefinition("flashHome", "any" +
	"Flash NFS directory path", String.class, new String(""));
	ParameterDefinition flashLocalOutput = new ParameterDefinition("flashLocalOutput", "any" +
	"Flash Local join files created here", String.class, new String(""));
	for (ParameterDefinition def: new ParameterDefinition[] {flashLocalInput, flashHome, flashLocalOutput, tempWritableFolder}) {
	      defs.put(def.getName(), def);
	}
	
	for (ParameterDefinition def:
	  ContrailParameters.getInputOutputPathOptions()) {
	  defs.put(def.getName(), def);
	}
    return Collections.unmodifiableMap(defs);
  }

  public RunningJob runJob() throws Exception {
    JobConf conf = new JobConf(createPairedReadsForFlash.class);
	conf.setJobName("Copying Joined files to Local and Running Flash ");
	String inputPath = (String) stage_options.get("inputpath");
	String junkPath = (String) stage_options.get("junkPath");
	String outputPath = (String) stage_options.get("outputpath");
    //Sets the parameters in JobConf
    initializeJobConfiguration(conf);
    AvroJob.setMapperClass(conf, RunFlashMapper.class);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(junkPath));
    ///Input is the joined mate pair file in Avro. 
    joinedfqrecord read = new joinedfqrecord();
    AvroJob.setInputSchema(conf, read.getSchema());
    //Map Only Job
    conf.setNumReduceTasks(0);
    // Delete the output directory if it exists already
    Path out_path = new Path(junkPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }	    
	// Delete the output directory if it exists already
	out_path = new Path(outputPath);
	if (FileSystem.get(conf).exists(out_path)) {
	  FileSystem.get(conf).delete(out_path, true);  
	}    
	FileSystem.get(conf).mkdirs(out_path);    
	long starttime = System.currentTimeMillis();            
	RunningJob result = JobClient.runJob(conf);
	long endtime = System.currentTimeMillis();
	float diff = (float) (((float) (endtime - starttime)) / 1000.0);
	System.out.println("Runtime: " + diff + " s");
	return result;	
  }
	
  public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new createPairedReadsForFlash(), args);
	System.exit(res);
	}
}
	

