package contrail.correct;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

import java.util.*;

/*
 * This class prepares quake run
 * Mapper -Writes local file after splitting up the joined record that it gets as input
 * Output files are written in blocks of 1000.
 * In the close(), correct is run on local files
 */
public class createPairedReadsForQuake extends Stage{
  final static int MAX= 2;
  public static int cutoff;
  static int KmerSize;
  static void writeLocalFile(ArrayList<String> a1,ArrayList<String> a2,String fp1, String fp2)
  {
    try{
		//First File
	  FileWriter fstream = new FileWriter(fp1,true);
	  BufferedWriter out = new BufferedWriter(fstream);
	  for(int i=0;i<a1.size();i++){
		out.write(a1.get(i)+"\n");
	  }
	  out.close();
	  fstream.close();
	  //Second File
	  fstream = new FileWriter(fp2,true);
	  out = new BufferedWriter(fstream);
	  for(int i=0;i<a2.size();i++){
		out.write(a2.get(i)+"\n");
	  }
	  out.close();
	  fstream.close();
 	}
	 catch (Exception e){
		 e.printStackTrace();
	}
  }
	
  /*This is similar to flash parallel invocation. FastQ files are written to 
   * local cluster nodes and quake is invoked in close()
   */
  public static class RunCorrectOnPairedMapper extends AvroMapper<joinedfqrecord, NullWritable>{
		
    String filePathFq1,filePathFq2;
	String localTime;
	private long K;
	private ArrayList<String> a1;
	private ArrayList<String> a2;
	private String QuakeHome;
    private String QuakeLocalInputFolderName = null;
    private String tempWritableFolder = null;	
    private String QuakeLocalInputFolderPath = null;
    private String CorrectedPairedHDFSOutPath = null;
	int count ;
	@Override
	public void configure(JobConf job) {
      Calendar calendar = Calendar.getInstance();
      java.util.Date now = calendar.getTime();
      java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
      int time = currentTimestamp.getNanos();
      createPairedReadsForQuake stage = new createPairedReadsForQuake();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      QuakeHome = (String)(definitions.get("QuakeHome").parseJobConf(job));
      QuakeLocalInputFolderName = (String)(definitions.get("QuakeLocalInput").parseJobConf(job));
      tempWritableFolder = (String)(definitions.get("tempWritableFolder").parseJobConf(job));
      K = (Integer)(definitions.get("K").parseJobConf(job));  
      QuakeLocalInputFolderPath = tempWritableFolder+"/"+QuakeLocalInputFolderName;
      CorrectedPairedHDFSOutPath = (String)(definitions.get("outputpath").parseJobConf(job));
      localTime = ""+time;
      /*Temporary Filenames get their name from the timestamp*/        
	  filePathFq1 = QuakeLocalInputFolderPath+"/"+time+"correct_1.fq";
	  filePathFq2 = QuakeLocalInputFolderPath+"/"+time+"correct_2.fq";
	  K = job.getLong("K", 0);
	  a1= new ArrayList<String>();
	  a2= new ArrayList<String>();
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
    a1.add(seqId+"/1\n"+seq1+"\n"+"+\n"+qval1);
    a2.add(seqId2+"/2\n"+seq2+"\n"+"+\n"+qval2);
    count ++;
 
    //We write to a local file in blocks of 10000 records
    if(count == 10000){
        writeLocalFile(a1,a2,filePathFq1,filePathFq2);  
        a2.clear();
        a1.clear();
        count =0;
    }
    output.collect(NullWritable.get());
  }
  
  @Override
  public void close() throws IOException{
    if(count > 0){
      writeLocalFile(a1,a2,filePathFq1,filePathFq2);
      count = 0;
    }
    runCorrectForPairedReads.correctRunner(filePathFq1, filePathFq2, localTime,K, QuakeHome, QuakeLocalInputFolderPath, CorrectedPairedHDFSOutPath);
  	//Deleting Local Temporary files created by the mapper
    File fp = new File(filePathFq1);
    if(fp.exists())fp.delete();
    fp = new File(filePathFq2);
    if(fp.exists())fp.delete();	
  }
}
	
protected Map<String, ParameterDefinition> createParameterDefinitions() {
	HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition QuakeLocalInput = new ParameterDefinition("QuakeLocalInput", "any" +
	   		"The name of the folder that is used to store splitted files which serve as " +
	   		"input for quake. We create this automatically within the tempFolder", String.class, new String(""));
    ParameterDefinition tempWritableFolder = new ParameterDefinition("tempWritableFolder", "any" +
		    		"The folder that is writable provided on every node in the" +
		    		"cluster with write permissions", String.class, new String(""));
    ParameterDefinition QuakeHome = new ParameterDefinition("QuakeHome", "any" +
		    		"Quake NFS directory path", String.class, new String(""));
    
    for (ParameterDefinition def: new ParameterDefinition[] {QuakeLocalInput, QuakeHome, tempWritableFolder}) {
	   	      defs.put(def.getName(), def);
	    }
	   
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
	      defs.put(def.getName(), def);
	}
	
    return Collections.unmodifiableMap(defs);
}
	
 public RunningJob runJob() throws Exception {
   JobConf conf = new JobConf(createPairedReadsForQuake.class);
   conf.setJobName("Copying Joined files to Local and Running Quake ");
   String inputPath = (String) stage_options.get("inputpath");
   String junkPath = (String) stage_options.get("junkPath");
   String outputPath = (String) stage_options.get("outputpath");
   ///Basic Initialization
   initializeJobConfiguration(conf);
   AvroJob.setMapperClass(conf, RunCorrectOnPairedMapper.class);
   FileInputFormat.addInputPath(conf, new Path(inputPath));
   FileOutputFormat.setOutputPath(conf, new Path(junkPath));
   /* Input is a joined schema which containes two mate files joined*/
   joinedfqrecord read = new joinedfqrecord();
   AvroJob.setInputSchema(conf, read.getSchema());
   //Map Only Job
   conf.setNumReduceTasks(0);
   // Delete the output directory if it exists already
   Path out_path = new Path(junkPath);
   if (FileSystem.get(conf).exists(out_path)) {
	      FileSystem.get(conf).delete(out_path, true);  
   }
	    
   out_path = new Path(outputPath);
   if (FileSystem.get(conf).exists(out_path)) {
     FileSystem.get(conf).delete(out_path, true);  
   }
   FileSystem.get(conf).mkdirs(out_path);
   long starttime = System.currentTimeMillis();            
   RunningJob runningJob = JobClient.runJob(conf);
   long endtime = System.currentTimeMillis();
   float diff = (float) (((float) (endtime - starttime)) / 1000.0);
   System.out.println("Runtime: " + diff + " s");
   return runningJob;
  }
  
 public static void main(String[] args) throws Exception {
   int res = ToolRunner.run(new Configuration(), new createPairedReadsForQuake(), args);
   System.exit(res);
  }
}