package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This file converts a part file of kmer count which is in Avro format into Non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff calculation
 */

public class convertAvroFlashOutToFlatFastQ extends Stage{  
	
	
	  /* Simply reads the file from HDFS and gives it to the reducer*/
	  
	 public static class flattenFastQMapper extends MapReduceBase
	  implements Mapper<AvroWrapper<fastqrecord>, NullWritable, Text, Text > {
		
		 public void map(AvroWrapper<fastqrecord> fastqData, NullWritable inputValue,
			        OutputCollector<Text, Text> output, Reporter reporter)
			            throws IOException {
			Text seqId  = new Text(fastqData.datum().getId().toString());
			String line  = fastqData.datum().getRead()+"\t"+fastqData.datum().getQvalue()+"\t";
			Text info = new Text(line);
			output.collect(seqId, info);
		 }
	 }
	
	
  /*The output schema is the pair schema of <kmer, count>*/
  public static class flattenFastQReducer extends MapReduceBase implements Reducer <Text, Text, Text, Text > {
    public void reduce(Text seq, Iterator<Text> info, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
	 	Text information = info.next();
        collector.collect(seq, information);
     
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
  
  public RunningJob runJob() throws Exception {
    //Here the inputFile is not for a directory, but a specific path 
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");		  
    JobConf conf = new JobConf(convertAvroFlashOutToFlatFastQ.class);
    conf.setJobName("Convert part file to non avro");
    
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
   
    AvroJob.setInputSchema(conf, fastqrecord.SCHEMA$);
    initializeJobConfiguration(conf);
    AvroInputFormat<fastqrecord> input_format = new AvroInputFormat<fastqrecord>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);
    //AvroJob.setMapOutputSchema(conf, new Pair<Text,Text>(new Text(""), new Text("")).getSchema()); 

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setNumReduceTasks(0);
    conf.setMapperClass(flattenFastQMapper.class);
    //conf.setReducerClass(flattenFastQReducer.class);
    //conf.setOutputKeyClass(Text.class);
    //conf.setOutputValueClass(Text.class);
	// Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
	  FileSystem.get(conf).delete(out_path, true);  
	}
	
    long starttime = System.currentTimeMillis();            
    RunningJob runingJob = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
	return runingJob;
  }							
   
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new convertAvroFlashOutToFlatFastQ(), args);
	System.exit(res);
  }
}


