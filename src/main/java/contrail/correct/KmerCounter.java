/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Avijit Gupta (mailforavijit@gmail.com)
package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
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

import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/*
 * This class counts Kmers. We find out Kmers within a line of input from the fastQ file, and 
 * emit it with a frequency of 1. This is combined at a reducer, which calculates
 * the Kmer counts
 */
public class KmerCounter extends Stage
{       
 /*The input schema to this mapper is the normal fastq schema
  * id, read, qvalue
  */
  public static class KmerCounterMapper extends AvroMapper<fastqrecord, Pair<Utf8, Long>>        
  { 
    // a global counter
    private static long K = 0;
    public void configure(JobConf job) {
      KmerCounter stage = new KmerCounter();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));
    }
    @Override
    public void map(fastqrecord compressed_read,AvroCollector<Pair<Utf8, Long>> output, Reporter reporter) throws IOException {
      String seq= compressed_read.getRead().toString();
      /* We convert every kmer to its canonical for the kmer counting phase so that
      * canonical kmers dont appear at different places
      */
      for (int i=0; i<= seq.length() - K; i++){
        String kmer = seq.substring(i,(int)(i+K));
        Sequence dnaSequence = new Sequence(kmer, DNAAlphabetFactory.create());
        Sequence canonicalSeq = DNAUtil.canonicalseq(dnaSequence);
        String kmerCanonical = canonicalSeq.toString();
        output.collect(new Pair<Utf8,Long>(new Utf8(kmerCanonical),1L));
      }
    }
  }
       
  public static class KmerCounterReducer extends AvroReducer<Utf8, Long, Pair<Utf8, Long> > {
  @Override
  public void reduce(Utf8 kmer, Iterable<Long> counts, AvroCollector<Pair<Utf8,Long>> collector, Reporter reporter) throws IOException {
    long sum = 0;
    for (long count : counts){
      sum += count;
    }
   collector.collect(new Pair<Utf8,Long>(kmer, sum));
  }
 } 

  
  public RunningJob runJob() throws Exception{
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    JobConf conf = new JobConf(KmerCounter.class);
    conf.setJobName("Kmer Counter ");
    initializeJobConfiguration(conf);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    fastqrecord read = new fastqrecord();
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroJob.setMapperClass(conf, KmerCounterMapper.class);
    AvroJob.setReducerClass(conf, KmerCounterReducer.class);
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }
    long starttime = System.currentTimeMillis();            
    RunningJob run = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return run;        
  }

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new KmerCounter(), args);
    System.exit(res);
  }
       
}
