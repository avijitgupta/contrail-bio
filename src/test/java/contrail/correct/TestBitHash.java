package contrail.correct;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import contrail.ReporterMock;
import contrail.correct.FilterKmerCountAndBuildBithash.FilterReducer;
import contrail.stages.AvroCollectorMock;
import contrail.stages.ParameterDefinition;

public class TestBitHash {

  private String mapTestData = "AAA 5 " + 
                               "ACC 2 " +
                               "ATT 1 " +
                               "ATG 10 " +
                               "AGT 6 " +
                               "CTA 13 " +
                               "TTA 9 ";
  
  private String reducerInData = "AAA 5 " +
                                 "TTT 5 " +
                                 "ATG 10 " +
                                 "CAT 10 " +
                                 "AGT 6 " +
                                 "ACT 6 " +
                                 "CTA 13 " +
                                 "TAG 13 " +
                                 "TTA 9 " + 
                                 "TAA 9 ";
                          
  private int cutoff = 4;
  
  private int K = 3;
  /**
   * Bithash representation in integer format
   */
  byte[] expectedBithash = {-127, 18, 16, 8 ,0, 0, -96, 9};

/**
 * Tests if the mapper is pruning correctly according to the cutoff
 * and if both the kmer and its RC are emitted
 */
  @Test
  public void testMap() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    FilterKmerCountAndBuildBithash.FilterMapper mapper = new FilterKmerCountAndBuildBithash.FilterMapper();
    FilterKmerCountAndBuildBithash stage = new FilterKmerCountAndBuildBithash();
    JobConf job = new JobConf(FilterKmerCountAndBuildBithash.FilterMapper.class);
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("cutoff").addToJobConf(job, cutoff);
    mapper.configure(job);
    StringTokenizer st = new StringTokenizer(mapTestData, " ");
    AvroCollectorMock<Pair<Utf8, Long>> collector_mock = new AvroCollectorMock<Pair<Utf8, Long>>();
    while(st.hasMoreTokens()){
      Pair<Utf8, Long> countPair = new  Pair<Utf8, Long>(new Utf8(""), new Long(0));
      String kmer = st.nextToken();
      Long count = Long.parseLong(st.nextElement().toString());
      countPair.set(new Utf8(kmer), count);
      try {
        mapper.map(
            countPair,
            collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }  
    }
    HashMap<String, Long> expectedHashMap = getExpectedOutput(reducerInData);
    assertOutput(collector_mock, expectedHashMap);
  }
  
  @Test
  public void testBithash() throws Exception {
    FilterReducer reducer = new FilterReducer();
    HashMap<String, Long> reducerInput = getExpectedOutput(reducerInData);
    JobConf job = new JobConf(FilterReducer.class);   
    FilterKmerCountAndBuildBithash stage = new FilterKmerCountAndBuildBithash();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("K").addToJobConf(job, K);
    String tempOutput = getTempDirectory().getAbsolutePath();
    definitions.get("outputpath").addToJobConf(job, tempOutput);
    reducer.configure(job);
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    AvroCollectorMock<Pair<Utf8, Long>> collector_mock =
        new AvroCollectorMock<Pair<Utf8, Long>>();
    ArrayList<String> sortedKeys=new ArrayList<String>(reducerInput.keySet());
    Collections.sort(sortedKeys);
    Iterator<String> iter = sortedKeys.iterator();
    //Execute reducer
    while(iter.hasNext()){
      String kmer = iter.next();
      Long counts =  reducerInput.get(kmer);
      ArrayList<Long> countList = new ArrayList<Long>();
      countList.add(counts);
      Utf8 Utf8kmer = new Utf8(kmer);
      try {
        reducer.reduce(
            Utf8kmer, countList, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
    }
    //triggers bithash computation on outputpath
    reducer.close();
    String bithashFile = new File(tempOutput, "bithash").getAbsolutePath();
    byte[] actualBithash = readBinaryFile(bithashFile);
    assertBithashOutput(actualBithash);
    File temp = new File(tempOutput);
    if(temp.exists()){
      FileUtils.deleteDirectory(temp);
    }
  }
  
  /** 
   * Read the given binary file, and return its contents as a byte array. 
   * @throws FileNotFoundException 
   */ 
  byte[] readBinaryFile(String aInputFileName) throws Exception{
    File file = new File(aInputFileName);
    byte[] result = new byte[(int)file.length()];
    FileInputStream inputStream = new FileInputStream(file);
    inputStream.read(result);
    return result;
  }
  
  /**
   * Asserts the correctness of the output
   * @param collector_mock
   * @param expectedHashMap
   */
  private void assertOutput(AvroCollectorMock<Pair<Utf8, Long>> collector_mock, HashMap<String, Long> expectedHashMap){
    Iterator<Pair<Utf8, Long>> iterator = collector_mock.data.iterator();
    int numberOfCountRecords = 0;
    while (iterator.hasNext()) {
      Pair<Utf8, Long> pair = iterator.next();
      String kmer = pair.key().toString();
      Long count = pair.value();
      //The kmer should always be present 
      assertFalse(expectedHashMap.get(kmer)==null);
      assertTrue(expectedHashMap.get(kmer) == count);
      numberOfCountRecords++;
    }
    assertEquals(expectedHashMap.size(),numberOfCountRecords);
  }
  
  private void assertBithashOutput(byte actualBithash[]){
    //checks if the length is the same
    assertEquals(actualBithash.length, expectedBithash.length);
    for(int i = 0 ; i < actualBithash.length ; i ++){
      assertEquals(actualBithash[i], expectedBithash[i]);
    }
  }
  
  private File getTempDirectory(){
    File temp = null;
    try {
      temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
    }
    if(!(temp.delete())){
        throw new RuntimeException(
            "Could not delete temp file: " + temp.getAbsolutePath());
    }
    if(!(temp.mkdir())) {
        throw new RuntimeException(
            "Could not create temp directory: " + temp.getAbsolutePath());
    }
    return temp;
  }
  
  private HashMap<String, Long> getExpectedOutput(String inputData){
    StringTokenizer st = new StringTokenizer(inputData, " ");
    HashMap<String, Long> expectedHashMap = new HashMap<String, Long>();
    while(st.hasMoreTokens()){
      String kmer = st.nextToken();
      Long count = Long.parseLong(st.nextElement().toString());
      expectedHashMap.put(kmer, count);
    }
    return expectedHashMap;
  }
}
