
/*
 * This file should be deprecated. Not for use now after Bithash Generation is done in
 * FilterGlobalCountFile
 * 
 */

package contrail.correct;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;


import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/*
 * Numeric Bithashes from all local runs are available in one directory.
 * The contents of all such files is ORed to generate the global bithash.
 * This is done in the reduce phase
 */
public class CreateGlobalBitHash extends Stage{

	public static class BitHashMapper extends MapReduceBase 
	    implements Mapper<Object, Text, LongWritable, LongWritable>{
		
		public void map(Object key, Text value, OutputCollector<LongWritable, LongWritable> output,
				Reporter reporter) 
				throws IOException {
			 
				String line = value.toString();
				StringTokenizer st = new StringTokenizer(line);
				long offset = Long.parseLong(st.nextToken());
				long bithash = Long.parseLong(st.nextToken());
				output.collect(new LongWritable(offset),new LongWritable(bithash));	
			 
		}
	
	}
	
	public static class BitHashReducer extends MapReduceBase implements Reducer
		<LongWritable,LongWritable,LongWritable,LongWritable> {
	    
		 
	 public void reduce(LongWritable key, Iterator<LongWritable> values, 
			 			OutputCollector <LongWritable, LongWritable> output,
			 			Reporter reporter) throws IOException{
	     
		    long count = 0;
		    while(values.hasNext()){
		  	LongWritable val = values.next();
		  	long bithash = Long.parseLong(val.toString());
		  		count = count | bithash;
		   }
		  	output.collect(key,new LongWritable(count));
	   }
	}

	protected Map<String, ParameterDefinition> createParameterDefinitions() {
	    HashMap<String, ParameterDefinition> defs =
	        new HashMap<String, ParameterDefinition>();
	    defs.putAll(super.createParameterDefinitions());
	
	    for (ParameterDefinition def:
	      ContrailParameters.getInputOutputPathOptions()) {
	      defs.put(def.getName(), def);
	    }
	    return Collections.unmodifiableMap(defs);
	  }
  

	public RunningJob runJob() throws Exception 
	{
	 	 String inputPath = (String) stage_options.get("inputpath");
		 String outputPath = (String) stage_options.get("outputpath");
		 JobConf conf = new JobConf(CreateGlobalBitHash.class);
	     conf.setJobName("Generating Global bithash");
	     FileInputFormat.addInputPath(conf, new Path(inputPath));
	     FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	     
		// job.setJarByClass(CreateGlobalBitHash.class);
		 conf.setMapperClass(BitHashMapper.class);
		 conf.setReducerClass(BitHashReducer.class);
		 conf.setOutputKeyClass(LongWritable.class);
		 conf.setOutputValueClass(LongWritable.class);
		 conf.setMapOutputKeyClass(LongWritable.class);
		 conf.setMapOutputValueClass(LongWritable.class); 

		 Path out_path = new Path(outputPath);
		 if (FileSystem.get(conf).exists(out_path)) {
	          FileSystem.get(conf).delete(out_path, true);  
	        }
	
		 long starttime = System.currentTimeMillis();            
	     RunningJob runningjob = JobClient.runJob(conf);
	     long endtime = System.currentTimeMillis();
	     float diff = (float) (((float) (endtime - starttime)) / 1000.0);
	     System.out.println("Runtime: " + diff + " s");
	     return runningjob;
	}

	  
  	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new CreateGlobalBitHash(), args);
	    System.exit(res);
	  }

}
