package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This file converts a part file of kmer count which is in Avro format into Non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff calculation
 */

public class ConvertFileToNonAvro extends Stage{  
  /* Simply reads the file from HDFS and gives it to the reducer*/
  public static class copyPartMapper extends MapReduceBase implements Mapper <AvroWrapper<Pair<Utf8, Long>>, NullWritable, Text, LongWritable> {

    @Override
    public void map(AvroWrapper<Pair<Utf8, Long>> key, NullWritable value,
        OutputCollector<Text, LongWritable> collector, Reporter reporter)
        throws IOException {
        Pair<Utf8, Long> kmerCountPair = key.datum();
        collector.collect(new Text(kmerCountPair.key().toString()), new LongWritable(kmerCountPair.value()));
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
    Pair<Utf8,Long> read = new Pair<Utf8,Long>(new Utf8(""), 0L);
    AvroJob.setInputSchema(conf, read.getSchema());
    conf.setJobName("Convert part file to non avro");
    AvroInputFormat<Pair<Utf8,Long>> input_format =
        new AvroInputFormat<Pair<Utf8,Long>>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    conf.setMapperClass(copyPartMapper.class);
    conf.setNumReduceTasks(0);
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
    int res = ToolRunner.run(new Configuration(), new ConvertFileToNonAvro(), args);
    System.exit(res);
  }
}
