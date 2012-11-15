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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.correct.KmerCounter.KmerCounterReducer;
import contrail.stages.AvroCollectorMock;
import contrail.stages.ParameterDefinition;

public class TestKmerCounter {

  private String fastqRecords= "@1/1 GCCACGAAATTAGC ?????????????? " +
                               "@2/1 TTATTAATCATTAA ?????????????? ";
  private int K = 10;

  //These kmers will be produced with a count of 1 if input is fastqRecords
  private String expectedKmers = "ATTTCGTGGC " +
                                 "AATTTCGTGG " + 
                                 "CACGAAATTA " +
                                 "ACGAAATTAG " + 
                                 "CGAAATTAGC " + 
                                 "TGATTAATAA " +
                                 "ATGATTAATA " +
                                 "AATGATTAAT " +
                                 "TAATGATTAA " +
                                 "TAATCATTAA " ;
  
  private String reducerInputKmers = "ATTTCGTGGC " +
                                     "ATTTCGTGGC " +
                                     "AATTTCGTGG " +
                                     "AATTTCGTGG " +
                                     "AATTTCGTGG " +
                                     "CACGAAATTA " + 
                                     "ACGAAATTAG ";
  
  @Test
  public void testRun() {
    File temp = getTempDirectory();
    File countsFile = new File(temp, "output");
    File avroFastqRecordInputFile = new File(temp, "fastqrecord.avro");
    runKmerCountTest(avroFastqRecordInputFile, countsFile);
  }
  
  /**
   * Tests the reducer. The input is taken as a string of kmers which are converted into 
   * a format compatible with reducer input. This is passes into the reducer and correctness is verified
   * using assertions.
   */
  @Test
  public void testReduce(){
    KmerCounterReducer reducer = new KmerCounterReducer();
    HashMap<String, ArrayList<Long>> reducerInput = getReducerHashMap(reducerInputKmers);
    HashMap<String, Long> reducedExpectedOutput = getExpectedOutput(reducerInputKmers);
    JobConf job = new JobConf(KmerCounterReducer.class);
    reducer.configure(job);
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    AvroCollectorMock<Pair<Utf8, Long>> collector_mock =
        new AvroCollectorMock<Pair<Utf8, Long>>();
    Iterator<String> iter = reducerInput.keySet().iterator();
    while(iter.hasNext()){
      String kmer = iter.next();
      ArrayList<Long> counts =  reducerInput.get(kmer);
      Utf8 Utf8kmer = new Utf8(kmer);
      try {
        reducer.reduce(
            Utf8kmer, counts, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
    }
    assertOutput(collector_mock,reducedExpectedOutput);
  }
  
  /**
   * Tests the mapper. 
   */
  @Test
  public void testMap(){
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    KmerCounter.KmerCounterMapper mapper = new KmerCounter.KmerCounterMapper();
    KmerCounter stage = new KmerCounter();
    JobConf job = new JobConf(KmerCounter.KmerCounterMapper.class);
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("K").addToJobConf(job, K);
    mapper.configure(job);
    StringTokenizer st = new StringTokenizer(fastqRecords, " ");
    AvroCollectorMock<Pair<Utf8, Long>> collector_mock = new AvroCollectorMock<Pair<Utf8, Long>>();
    while(st.hasMoreTokens()){
      fastqrecord record = new fastqrecord();
      record.setId(st.nextToken());
      record.setRead(st.nextToken());
      record.setQvalue(st.nextToken());
      try {
        mapper.map(
            record,
            collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }  
    }
    HashMap<String, Long> expectedHashMap = getExpectedOutput(expectedKmers);
    assertOutput(collector_mock, expectedHashMap);
  }
  
  /**
   * Returns an arraylist of 1's where the size of arraylist is the number of times
   * the kmer appers in reducerInput. This is used to mock the recuder input
   * Eg: ATA
   *     AAT
   *     ATG
   *     ATA
   *     
   *     will produce
   *     ATA 1 1
   *     AAT 1
   *     ATG 1
   * @param reducerInput
   * @return
   */
  private HashMap<String, ArrayList<Long>> getReducerHashMap(String reducerInput){
    StringTokenizer st = new StringTokenizer(reducerInput, " ");
    HashMap<String, ArrayList<Long>> expectedOutput = new HashMap<String, ArrayList<Long>>();
    while(st.hasMoreTokens()){
      String kmer = st.nextToken();
      ArrayList<Long> initList;
      if(expectedOutput.get(kmer) == null){
        initList = new ArrayList<Long>();
      }
      else{
        initList = expectedOutput.get(kmer);
      }
      initList.add(1L);
      expectedOutput.put(kmer, initList);
    }
    return expectedOutput;
  }
  
  /**
   * Asserts the correctness of the output
   * @param collector_mock
   * @param expectedHashMap
   */
  private void assertOutput(AvroCollectorMock<Pair<Utf8, Long>> collector_mock, HashMap<String, Long> expectedHashMap){
    Iterator<Pair<Utf8, Long>> iterator = collector_mock.data.iterator();
    while (iterator.hasNext()) {
      Pair<Utf8, Long> pair = iterator.next();
      String kmer = pair.key().toString();
      Long count = pair.value();
      //The kmer should always be present 
      assertFalse(expectedHashMap.get(kmer)==null);
      long newcount = expectedHashMap.get(kmer);
      newcount-=count;
      assertTrue(newcount>=0);
      expectedHashMap.put(kmer, newcount);
    }
    //everything should be exactly zero 
    Iterator<String> iter = expectedHashMap.keySet().iterator();
    while(iter.hasNext()) {
      String kmer = iter.next().toString();
      Long count = (Long)expectedHashMap.get(kmer);
      assertTrue(count==0);
    }

  }

  /**
   * Gets a temporary directory
   * @return
   */
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
  
  private void runKmerCountTest(File kmerCountInput, File outputPath){
    writeDataToFile(kmerCountInput);
    runApp(kmerCountInput, outputPath);
  }
  
  private HashMap<String, Long> getExpectedOutput(String inputData){
    StringTokenizer st = new StringTokenizer(inputData, " ");
    HashMap<String, Long> expectedHashMap = new HashMap<String, Long>();
    while(st.hasMoreTokens()){
      String kmer = st.nextToken();
      if(expectedHashMap.get(kmer)==null){
        expectedHashMap.put(kmer, 1L);
      }
      else{
        long count = expectedHashMap.get(kmer);
        count ++;
        expectedHashMap.put(kmer, count);
      }
    }
    return expectedHashMap;
  }
  
  /**
   * writes fastqrecord data to outFile in avro format
   * @param outFile
   */
  private void writeDataToFile(File outFile){
    Schema schema = (new fastqrecord()).getSchema();
    DatumWriter<fastqrecord> datum_writer =
        new SpecificDatumWriter<fastqrecord>(schema);
    DataFileWriter<fastqrecord> writer =
        new DataFileWriter<fastqrecord>(datum_writer);
    StringTokenizer st = new StringTokenizer(fastqRecords, " ");
    try {
      writer.create(schema, outFile);
      while(st.hasMoreTokens()){
        fastqrecord record = new fastqrecord();
        record.setId(st.nextToken());
        record.setRead(st.nextToken());
        record.setQvalue(st.nextToken());
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing to an avro file. Exception:" +
          exception.getMessage());
    }
  }
  
  /**
   * Runs the application
   * @param input
   * @param output
   */
  private void runApp(File input, File output){
    KmerCounter kmerCounter = new KmerCounter();
    String[] args =
      {"--inputpath=" + input.toURI().toString(),
       "--outputpath=" + output.toURI().toString(),
       "--K="+ K};

    try {
      kmerCounter.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
