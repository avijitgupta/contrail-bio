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
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

import java.util.*;

import java.util.Date;

/*
 * This class runs correct on singles
 * Mapper - Writes the input avro format fastq file into normal format on local disk
 * Close() invokes the correct program
 */
public class CorrectSinglesInvocationStub extends Stage{
  final static int MAX= 2;
  static int cutoff;
  static int KmerSize;
  static void writeLocalFile(ArrayList<String> a, String filePath)
  {
	try{
	  FileWriter fstream = new FileWriter(filePath,true);
	  BufferedWriter out = new BufferedWriter(fstream);
	  for(int i=0;i<a.size();i++){
		out.write(a.get(i)+"\n");
	  }
	  out.close();
	  fstream.close();
	  }
     catch (Exception e)	{
    	 e.printStackTrace();
    }
  }
	
  public static class RunCorrectOnSinglesMapper  extends AvroMapper<fastqrecord, NullWritable> {	
    private int idx = 0;
	private String line1 = null;
	private String line2  = null;
	private String line3 = null;
	private String line4  = null;
    private String filePath=null;
    private String localTime = null;
    private String QuakeHome = null;
    private String QuakeLocalInputFolderName = null;
    private String tempWritableFolder = null;	
    private String QuakeLocalInputFolderPath = null;
    private String CorrectedSinglesHDFSOutPath = null;
    private long K = 0;
    private ArrayList<String> temp_arraylist;
    int count = 0;
    int flagStarting;
    
    @Override
    public void configure(JobConf job) 
    {
      CorrectSinglesInvocationStub stage= new CorrectSinglesInvocationStub();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      Calendar calendar = Calendar.getInstance();
      java.util.Date now = calendar.getTime();
      java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
      localTime = ""+currentTimestamp.getNanos();
      QuakeHome = (String)(definitions.get("QuakeHome").parseJobConf(job));
      QuakeLocalInputFolderName = (String)(definitions.get("QuakeLocalInput").parseJobConf(job));
      tempWritableFolder = (String)(definitions.get("tempWritableFolder").parseJobConf(job));
      K = (Integer)(definitions.get("K").parseJobConf(job));  
      QuakeLocalInputFolderPath = tempWritableFolder+"/"+QuakeLocalInputFolderName;
	  File tempDirectory = new File(QuakeLocalInputFolderPath);
	  if(!tempDirectory.exists()){
			tempDirectory.mkdir();
	  }
	  //hadoophome = job.get("hadoophome");
      CorrectedSinglesHDFSOutPath = (String)(definitions.get("outputpath").parseJobConf(job));
      temp_arraylist = new ArrayList<String>();
	  filePath = QuakeLocalInputFolderPath+"/"+localTime+"_quakesingle.fq";
  }
    
    public void map(fastqrecord fq_record, 
            AvroCollector<NullWritable> output, Reporter reporter) throws IOException {
      line1 = fq_record.getId().toString();
      line2 = fq_record.getRead().toString();
      line3 = "+";
      line4 = fq_record.getQvalue().toString();;
      temp_arraylist.add(line1+"\n"+line2+"\n"+line3+"\n"+line4);
      count ++;
      if(count==10000)
      {
      	 CorrectSinglesInvocationStub.writeLocalFile(temp_arraylist,filePath);
    	 temp_arraylist.clear();
    	 count =0;	
      }
      output.collect(NullWritable.get());
   }
 
@Override
  public void close() throws IOException{
	if(count > 0){
	  CorrectSinglesInvocationStub.writeLocalFile(temp_arraylist,filePath);
	  temp_arraylist.clear();
      count =0;
	}
	//CorrectSinglesExec.runcode(filePath,localTime,K,QuakeHome,QuakeLocalInputFolderPath,CorrectedSinglesHDFSOutPath);
		
		//deleting Local File
	File fp = new File(filePath);
	if(fp.exists())fp.delete();
  }
}

	
	protected Map<String, ParameterDefinition> createParameterDefinitions() {
	  HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
	  defs.putAll(super.createParameterDefinitions());
	  ParameterDefinition QuakeLocalInput = new ParameterDefinition("QuakeLocalInput", "any" +
					"The name of the folder that is used to store local fastq files which serve as " +
					"input for quake. We create this automatically within the tempWritableFolder",
					String.class, new String(""));
	  ParameterDefinition tempWritableFolder = new ParameterDefinition("tempWritableFolder", "any" +
			    		"The folder that is writable provided on every node in the" +
			    		"cluster with write permissions", String.class, new String(""));
	  ParameterDefinition QuakeHome = new ParameterDefinition("QuakeHome", "any" +
			    		"Quake NFS directory path", String.class, new String(""));
	  ParameterDefinition cutoff = new ParameterDefinition("cutoff", "any" +
					"The cutoff value which is obtained by running quake on the " +
					"Kmer count part file", Integer.class, 0);
	  for (ParameterDefinition def: new ParameterDefinition[] {QuakeLocalInput, QuakeHome, tempWritableFolder, cutoff}) {
				      defs.put(def.getName(), def);
	  }
	  for (ParameterDefinition def:
			  ContrailParameters.getInputOutputPathOptions()) {
			  defs.put(def.getName(), def);
	  }
	  return Collections.unmodifiableMap(defs);
	}


	public RunningJob runJob() throws Exception
	{
	  JobConf conf = new JobConf(CorrectSinglesInvocationStub.class);
	  // Basic Initialisation	
	  conf.setJobName("Running Correct Local ");
	  //This contains fastQ singles input folder
      String inputPath = (String) stage_options.get("inputpath");
      //This is a junk path for Null Writable output
      String junkPath = (String) stage_options.get("junkPath");
      //This is the path on HDFS where the corrected files will be copied into
      String outputPath = (String) stage_options.get("outputpath");
      initializeJobConfiguration(conf);
	  conf.setInt("mapred.task.timeout", 0);
      AvroJob.setMapperClass(conf, RunCorrectOnSinglesMapper.class);
      FileInputFormat.addInputPaths(conf, inputPath);
      FileOutputFormat.setOutputPath(conf, new Path(junkPath));
      fastqrecord read = new fastqrecord();
      AvroJob.setInputSchema(conf, read.getSchema());
      conf.setNumReduceTasks(0); 
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
	    int res = ToolRunner.run(new Configuration(), new CorrectSinglesInvocationStub(), args);
	    System.exit(res);
	}
}
