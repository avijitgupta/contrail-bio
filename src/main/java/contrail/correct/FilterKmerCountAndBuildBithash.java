package contrail.correct;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import java.util.*;

/* The mapper prunes the count file according to the cutoff obtained
 * The mapper reads in Avro records and emits out avro records if the value
 * of count is greater than cutoff. 
 * The reducer builds the bithash in a serial manner. Since the kmers that reach the reducer are sorted,
 * we can write out the bithash in a serial manner either directly on a file, or within an 
 * in memory store that is finally written out to a HDFS location 
 */
  public class FilterKmerCountAndBuildBithash extends Stage {
    static long previousKmerIndex = 0;
    static int correctionK;
    static List<Byte> bitvector;
    public static class FilterMapper 
    extends AvroMapper<Pair<Utf8, Long>, Pair<Utf8, Long>> {	
      private int cutOff;
      public void configure(JobConf job){ 
        FilterKmerCountAndBuildBithash stage = new FilterKmerCountAndBuildBithash();
        Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
        cutOff = (Integer)(definitions.get("cutoff").parseJobConf(job));
      }
  
  //incoming key,value pairs - (kmer, frequency)
  public void map(Pair<Utf8, Long> count_record, 
                  AvroCollector<Pair<Utf8, Long>> output, Reporter reporter) throws IOException {
    long freq_kmer = count_record.value().longValue();
    if(freq_kmer>= cutOff) {
      String kmer = count_record.key().toString();
      Sequence dnaSequence = new Sequence(kmer, DNAAlphabetFactory.create());
      Sequence revComplement = DNAUtil.reverseComplement(dnaSequence);
      String rev_complement  =revComplement.toString();
      Pair<Utf8, Long> rc_out = new Pair<Utf8, Long>(rev_complement, freq_kmer);
      //We emit both the kmer and its reverse complement so that both of them are set
      // in the bitvector
      output.collect(count_record); 
      output.collect(rc_out); 
    }
   }
  }
  
  /*
   * A single instance of this reducer is launched. This creates the bithash, one character at a time.
   * Every Kmer has an index in this bithash, and the index is governed by the sorted order
   * in which the Kmer appears. We calculate the index of the incoming Kmer and compare it
   * to the index of  the previous kmer. If the new kmer lies within the same character (8 locations) in which
   * the previous Kmer lies, the the new character is added too that character. Otherwise, the previous
   * character is finalised, and zero characters are added till the character (8 locations) that contain
   * the incoming Kmer is reached.
   */
    
  public static class FilterReducer extends AvroReducer<Utf8, Long, Pair<Utf8,Long> > {	
    static int ch;
    private static String bithashFile;
    public void configure(JobConf job){ 
      ch = 0;
      bitvector = new ArrayList<Byte>();
      FilterKmerCountAndBuildBithash stage = new FilterKmerCountAndBuildBithash();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      correctionK = (Integer)(definitions.get("K").parseJobConf(job));
      String outPath = (String)(definitions.get("outputpath").parseJobConf(job));
      bithashFile = new File(outPath, "bithash").getAbsolutePath();
    }
  
  //incoming key,value pairs - (kmer, frequency)
  public void reduce(Utf8 kmer , Iterable<Long> count, 
                     AvroCollector<Pair<Utf8,Long>> output, Reporter reporter) throws IOException {
    Pair<Utf8,Long> out = new Pair<Utf8,Long>("", 0);;
    out.set(kmer, count.iterator().next());
    output.collect(out);
    long kmerIndex = getKmerIndex(kmer);
    AddRelevantCharactersToBitvector(kmerIndex);
  }
	 
  public void AddRelevantCharactersToBitvector(long kmerIndex){
    //The new Kmer is outside the current character that we have to write
    if((kmerIndex - previousKmerIndex )> (7 - previousKmerIndex%8)){
      while((previousKmerIndex+1)%8 !=0){
        ch = ch <<1 ;
        previousKmerIndex++;
      }
      bitvector.add((byte)ch);
      long numZeroChars = (kmerIndex - previousKmerIndex -1)/8;
      for(int i = 0 ; i < numZeroChars; i++){
        ch = 0;
        bitvector.add((byte)ch);
        previousKmerIndex+=8;
      }
      while(previousKmerIndex <kmerIndex){
        previousKmerIndex++; 
      }
      ch = 1;
    }
    else {
      while(previousKmerIndex < kmerIndex){
        ch = ch<<1;
        previousKmerIndex++; 
    }
    ch = ch | 1;
  }
}

  /*
   * Given the kmer, this method calculates its position in the bithash
   */
  public long getKmerIndex(Utf8 kmer){
    String kmerString = kmer.toString();
    long kmerIndex = 0;
    for(int i = 0 ; i <kmer.length(); i ++){
      kmerIndex <<= 2;
      kmerIndex |=getAlphabetValue(kmerString.charAt(i));
    }
    return kmerIndex;
  }

  /* this method assigns numeric value to each character, it the sorted order*/
  public int getAlphabetValue(char ch){
    if(ch=='a' || ch =='A') return 0;
    else if (ch=='c' || ch =='C') return 1;
    else if(ch=='g' || ch =='G') return 2;
    else return 3;
  }
		 
  public void close(){
    long bitVectorCount = (long)Math.pow(4, correctionK) - 1;
    if(previousKmerIndex < bitVectorCount){
      AddRelevantCharactersToBitvector(bitVectorCount);
      //removing the last bit set, because it's kmer was not present
      ch = ch ^ 1;
      bitvector.add((byte)ch);
    }
    // The last Kmer is present
    else if(previousKmerIndex == bitVectorCount){
      bitvector.add((byte)ch);
    }
    try{
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      Path bithashPath = new Path(bithashFile);
      if(fs.exists(bithashPath)){
        fs.delete(bithashPath, true);
      }
      FSDataOutputStream out = fs.create(bithashPath);
      for(int i = 0 ; i < bitvector.size(); i++){	
        out.write((byte)bitvector.get(i));
      }
      out.close();
    }
    catch(Exception e){
    	e.printStackTrace();
    }
  } 	
}

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition cutoff = new ParameterDefinition("cutoff", "any" +
    		                     "The cutoff value which is obtained by running quake on the " +
                                     "Kmer count part file", Integer.class, 0);
    for (ParameterDefinition def: new ParameterDefinition[] {cutoff}) {
      defs.put(def.getName(), def);
    }    
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
       defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }	
    
  public RunningJob runJob() throws Exception{
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = "ContrailPlus/Junk";
    String bithashPath = (String) stage_options.get("outputpath");
    JobConf conf = new JobConf(FilterKmerCountAndBuildBithash.class);
    conf.setJobName("Filter Kmer Counts ");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    initializeJobConfiguration(conf);
    AvroJob.setInputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroJob.setMapOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroJob.setOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroJob.setMapperClass(conf, FilterMapper.class);
    AvroJob.setReducerClass(conf, FilterReducer.class);
    conf.setNumReduceTasks(1);
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }
    out_path = new Path(bithashPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }
    FileSystem.get(conf).mkdirs(out_path);
    long starttime = System.currentTimeMillis();            
    RunningJob runningjob = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return runningjob;
  }
	 
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FilterKmerCountAndBuildBithash(), args);
    System.exit(res);
  }
}
