package contrail.correct;
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

/**
 * This file converts a part file of kmer count which is in Avro format into Non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff calculation
 */

public class convertToNewMateSchema extends Stage{  
  /* Simply reads the file from HDFS and gives it to the reducer*/
  public static class convertMapper extends AvroMapper<joinedfqrecord, MatePair> {
    @Override
      public void map(joinedfqrecord joined_r, AvroCollector<MatePair> output, Reporter reporter) throws IOException {
       // output.collect(count_record); 
    	MatePair outrecord = new MatePair();
    	outrecord.setLeft(new fastqrecord());
    	outrecord.setRight(new fastqrecord()); 
    	outrecord.getLeft().setId(joined_r.getId1()+"/1");
    	outrecord.getLeft().setQvalue(joined_r.getQvalue1());
    	outrecord.getLeft().setRead(joined_r.getRead1());
    	outrecord.getRight().setId(joined_r.getId2()+"/2");
    	outrecord.getRight().setQvalue(joined_r.getQvalue2());
    	outrecord.getRight().setRead(joined_r.getRead2());
    	output.collect(outrecord);
      }
  }
     
  /*The output schema is the pair schema of <kmer, count>*/
 /* public static class copyPartReducer extends MapReduceBase implements Reducer <AvroKey<Utf8>, AvroValue<Long>, Text, LongWritable > {
    public void reduce(AvroKey<Utf8> kmer, Iterator<AvroValue<Long>> counts, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException {
      while(counts.hasNext()) {
        AvroValue<Long> count = counts.next();
        collector.collect(new Text(kmer.datum().toString()), new LongWritable(count.datum()));
      }
    }
  } 
*/
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
    JobConf conf = new JobConf(convertToNewMateSchema.class);
    conf.setJobName("Convert schema to new");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    AvroJob.setInputSchema(conf, new joinedfqrecord().getSchema());
    AvroJob.setMapOutputSchema(conf, new MatePair().getSchema()); 
    AvroJob.setMapperClass(conf, convertMapper.class);
    conf.setNumReduceTasks(0);
    //AvroJob.setInputSchema(conf, new materecord().getSchema());
    AvroJob.setOutputSchema(conf, new MatePair().getSchema()); 
  //  conf.setReducerClass(copyPartReducer.class);
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
    int res = ToolRunner.run(new Configuration(), new convertToNewMateSchema(), args);
    System.exit(res);
  }
}
