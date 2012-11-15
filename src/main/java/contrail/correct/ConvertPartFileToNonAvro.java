package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This file converts a part file of kmer count which is in Avro format into Non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff calculation
 */

public class ConvertPartFileToNonAvro extends Stage{  
  /* Simply reads the file from HDFS and gives it to the reducer*/
  public static class copyPartMapper extends AvroMapper<Pair<Utf8, Long>, Pair<Utf8, Long>> {
    @Override
      public void map(Pair<Utf8, Long> count_record, AvroCollector<Pair<Utf8, Long>> output, Reporter reporter) throws IOException {
        output.collect(count_record); 
      }
  }
     
  /*The output schema is the pair schema of <kmer, count>*/
  public static class copyPartReducer extends MapReduceBase implements Reducer <AvroKey<Utf8>, AvroValue<Long>, Text, LongWritable > {
    public void reduce(AvroKey<Utf8> kmer, Iterator<AvroValue<Long>> counts, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException {
      while(counts.hasNext()) {
        AvroValue<Long> count = counts.next();
        collector.collect(new Text(kmer.datum().toString()), new LongWritable(count.datum()));
      }
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
    AvroJob.setInputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroJob.setMapOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema()); 
    AvroJob.setMapperClass(conf, copyPartMapper.class);
    conf.setReducerClass(copyPartReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
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
