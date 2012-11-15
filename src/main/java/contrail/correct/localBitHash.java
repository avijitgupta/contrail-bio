package contrail.correct;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;

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
 * This file should be deprecated. Not for use now after Bithash Generation is done in
 * FilterGlobalCountFile
 * 
 */


/*
 * The input to this class is the counts file. 
 * This counts file is emitted by mappers on their local fs
 * The local fs contains quake which we have modified to give out 
 * numeric bithases. These numeric bithashes are generated locally and later
 * are copied into the common directory on which mapreduce task is run
 * to get the global numeric bithash. This still needs to be converted into
 * quakecompatiblebithash.
 */

public class localBitHash extends Stage {

    public static String Input;
    static FileWriter fstream;
    static BufferedWriter out;

        
	static void writeLocalFile(ArrayList<String> a)
	{
		try{
			  		
			//  
			  for(int i=0;i<a.size();i++)
			  {
				  out.write(a.get(i)+"\n");
			  }
			  
			}
			  catch (Exception e)	{e.printStackTrace();}
	}
	
    
public static class BitHashMapper 
       extends AvroMapper<Pair<Utf8, Long>, NullWritable>
  {	
	private  ArrayList<String> temp_arraylist;
	private String QuakeLocalInputFolderName;
	private int count;
	private String localtime;
	private String filepath;
    private long cutoff;
    private long K;
    private String partialBithashHDFSOutput;
  //  private String hadoophome;
    private String QuakeHome;
	private String tempWritableFolder;
	private String QuakeLocalInputFolderPath;
    
    public void configure(JobConf job) 
    {
    	Calendar calendar = Calendar.getInstance();
        java.util.Date now = calendar.getTime();
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
        int time = currentTimestamp.getNanos();
        localtime = ""+time;
        
        localBitHash stage= new localBitHash();
        Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
        QuakeLocalInputFolderName = (String)(definitions.get("QuakeLocalInput").parseJobConf(job));
        K = (Integer)(definitions.get("K").parseJobConf(job));
        tempWritableFolder = (String)(definitions.get("tempWritableFolder").parseJobConf(job));
        QuakeHome = (String)(definitions.get("QuakeHome").parseJobConf(job));
        cutoff = (Integer)(definitions.get("cutoff").parseJobConf(job));
        partialBithashHDFSOutput = (String)(definitions.get("outputpath").parseJobConf(job));
        QuakeLocalInputFolderPath = tempWritableFolder+"/"+QuakeLocalInputFolderName;
        File dir = new File(QuakeLocalInputFolderPath);
        //remember to delete this shit
        if(!dir.exists())
        	dir.mkdir();

		filepath = QuakeLocalInputFolderPath+"/"+localtime+"_localbithash";
		temp_arraylist = new ArrayList<String>();

		try
		{
			fstream = new FileWriter(filepath,true);
			out = new BufferedWriter(fstream);
		}
		catch(Exception e)
		{

			System.out.print("Caught exception "+e.toString()+ " " + fstream);
		}
    	
    }
    
   
	
	//incoming key,value pairs - (kmer, frequency)
    public void map(Pair<Utf8, Long> count_record, 
            AvroCollector<NullWritable> output, Reporter reporter) throws IOException {
    
	    String kmer;
	    String frequency;
	    kmer= count_record.key().toString();
	    frequency = count_record.value().toString();
	 
	    temp_arraylist.add(kmer+"\t"+frequency);
	    count++;
	    		
	    if(count>=1000)
	    {
	    	
	    	localBitHash.writeLocalFile(temp_arraylist);  
	    	temp_arraylist.clear();
	    	count = 0;
	    }
    	output.collect(NullWritable.get());
    }
    
    
    public void close() throws IOException
	{
    	if(count>0)
    	{
    		localBitHash.writeLocalFile(temp_arraylist);  
    	}
    	
    	try{
    	  out.close();
		  fstream.close();
    	}catch(Exception e){}
    
    	try{

    		String q = QuakeHome+"/src/build_bithash -k " + K + " -c "+cutoff+" -m "+filepath+" -o "+filepath+"_out"; 
		
    		
			System.out.println(q);
	     	Process p = Runtime.getRuntime().exec(q);
	     	try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
     	
     
		
	     	Configuration conf2 = new Configuration();
		    FileSystem fs = FileSystem.get(conf2);
		    //Copying the local Bithash file into a common folder in HDFS
		    Path fp1 = new Path(filepath+"_out");
		    Path fp2 = new Path(partialBithashHDFSOutput);
		    fs.copyFromLocalFile(fp1, fp2);
		    ///Cleaning up local files
		    File fp = new File(filepath);
			if(fp.exists())fp.delete();
			
			fp = new File(filepath+"_out");
			if(fp.exists())fp.delete();
    	}
		 catch (IOException e) {e.printStackTrace(); }
    
    }
  }
    	
protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

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
		JobConf conf = new JobConf(KmerCounter.class);
	    conf.setJobName("Generating Local Bithashes ");
		///Basic Initialization
	    
	    //This contains the count file
	    String inputPath = (String) stage_options.get("inputpath");
	    //This is a junk path for Null Writable output
	    String junkPath = (String) stage_options.get("junkPath");
	    //This is the path on HDFS where local bithashes will be copied into
	    String outputPath = (String) stage_options.get("outputpath");
	    
	    
	    FileInputFormat.addInputPath(conf, new Path(inputPath));
	    FileOutputFormat.setOutputPath(conf, new Path(junkPath));
	    ///Input is the kmer count file in Avro. So we used Kmer count recrod schema
	    AvroJob.setInputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
	
	    AvroJob.setMapperClass(conf, BitHashMapper.class);
	
	       
	    initializeJobConfiguration(conf);
	    
	    //Map Only Job
	    conf.setNumReduceTasks(0);
	 // Delete the output directory if it exists already
	    Path out_path = new Path(junkPath);
	    if (FileSystem.get(conf).exists(out_path)) {
	      FileSystem.get(conf).delete(out_path, true);  
	    }
	    
	    //The Bit Hash Temporary directory on HDFS should not exist initially
	    Path bitHashPartHDFSOut = new Path(outputPath);
	    if (FileSystem.get(conf).exists(bitHashPartHDFSOut)) {
		      FileSystem.get(conf).delete(bitHashPartHDFSOut, true);  
		    }
	    
	    //Create the bithash temporary directory
	    FileSystem.get(conf).mkdirs(bitHashPartHDFSOut);
	   
	    long starttime = System.currentTimeMillis();            
	    RunningJob runningJob = JobClient.runJob(conf);
	    long endtime = System.currentTimeMillis();
	    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
	    System.out.println("Runtime: " + diff + " s");
	    return runningJob;
	}

	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new localBitHash(), args);
	    System.exit(res);
	  }
  
  }