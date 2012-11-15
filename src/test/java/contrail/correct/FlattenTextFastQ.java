package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;


// MapReduce Pass to Read Data in Sets of four lines (corresponding to each read),
//and write them out in one single line..
//////////////////////////////////////////////

public class FlattenTextFastQ extends Stage{
    
    
    public static class FlattenFastQMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
  	{	
		private int idx;
		private String line1 = null;
		private String line2  = null;
		private String line4  = null;
	    public void configure() 
		{
	    	idx = 0;
		}
	    
	    public void map(LongWritable lineid, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
	    
		    idx++;
		    String line = value.toString();
		    if(idx == 1){line1 = line;}
		    if(idx == 2){line2 = line;}
		    if(idx == 3){}//Plus Not Needed
		    if(idx == 4)
		    {
		    	line4 = line;
		    	int ind;
		    	ind = line1.indexOf(' ');
		    	if(ind==-1)ind = line1.length();
		    	line1 = line1.substring(0,ind);
		    	String flattenedInformation = line2+"\t"+line4+"\t";
		       	Text word = new Text();
		       	idx = idx%4;
		    	word.set(line1);
		    	Text information = new Text();
		    	information.set(flattenedInformation);
		    	
		        output.collect(word, information);
		    }
		}  

	
	}

	//Reducer simply reads data and writes in sorted order.
	
public static class FlattenFastQReducer extends MapReduceBase 
implements Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, Text> output, Reporter reporter)	throws IOException 
	{
		output.collect(key, value.next());
    }
}

protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
	defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
	          defs.put(def.getName(), def);
	}
	return Collections.unmodifiableMap(defs);
  }

  @SuppressWarnings("deprecation")
  public RunningJob runJob() throws Exception {
	
	    //Here the inputFile is not for a directory, but a specific path 
	String inputPath = (String) stage_options.get("inputpath");
	String outputPath = (String) stage_options.get("outputpath");	
	JobConf conf = new JobConf(FlattenTextFastQ.class);
	conf.setJobName("Flatten Raw FastQ files");
    conf.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4
    
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    
    conf.setMapperClass(FlattenFastQMapper.class); 
    conf.setReducerClass(FlattenFastQReducer.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    // The output of this stage is input to MapSide Join, 
    // for which data has to be sorted. For sorting, we keep only 1 reducer.
    
    conf.setNumReduceTasks(1);
    Configuration conf2 = new Configuration();
    FileSystem fs = FileSystem.get(conf2);
    Path fp = new Path(outputPath);
    if (fs.exists(fp)) {
    	   // remove the file first
    	         fs.delete(fp);
    	       }

    long starttime = System.currentTimeMillis();            
    RunningJob runingJob = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
	return runingJob;
  }
  
  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new FlattenTextFastQ(), args);
		System.exit(res);
	  }
}
	

