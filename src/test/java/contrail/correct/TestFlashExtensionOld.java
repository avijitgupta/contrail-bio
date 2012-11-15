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
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import contrail.stages.AvroCollectorMock;

public class TestFlashExtensionOld {
  //5 records - added a \ at 27:212, 22:239 & 27:226 because of presence of "
  private String joinedFastq = 
      "@SRR022868.8762453/1 TAATTTAACTTTGTCTGATAATTGTACGCTTAAATCAACGTCTTTCGGTAAAATATCAGATTGTGCTGGAATTAATAACACATCATCAACCGTTAATGATT II0IIII8IIIIIIIII=I,IIIIIIIIIII,1+II4)HIBIIII?IIE6*(1I1;I,E&I+I9I5/IA&&3=+#I)%+(5*2&&/0%+#\"/$(%$*0%(+ " + 
      "@SRR022868.8762453/2 AGGCTGTTTTTAATGTGGGAAAGTAAATTTGCAAAAGAATCATTAACGTTTGATGATGTGTTATTAATTCCAGCACAATCTGATATTTTACCGAAAGACGT CIIIIIIIIIII>IIIIII&,5II+(*IIIID:*&5I4.II*II.)CI5AI;5IC+0I%1(6&26&-,.)+#2.*3)%-.0-$'%&'&)%)&,$##&#+'+ " + 
      "@SRR022868.8762473/1 TTTTTATTTAAATTAATCATATAATTGCGAGGAGAATATTATGGATTTCGTTAATAATGATACAAGACAAATTGCTAAAAACTTATTAGGTGTCAAAGTGA IIIIIIII=@=FIIIIIIIIIIIIIIIII3II%I>/I,II=III2IIIIII:<1F/7I<I5B<4-I*9;&-A)I?@,$*)')/020+(;/#@+,,#%6%?' " + 
      "@SRR022868.8762473/2 TTCCACGATGTAGCCTGTATACGTTTGAGTGGTATCCTGATAAATCACTTTGACACCTAATAAGTTTTTAGCAATTTGTCTTGTATCATTATTAACGAAAT IIIIIIIIIIIIIIIIGIIII4I5GII.I)II1&9F/II&:*I&I62(744I723.3=>/90&@5?1,<&1-,'.,/,&-2*+.&&+%/)(--$#(,%##* " + 
      "@SRR022868.8762487/1 AGTAATATTGTTGCGTCTGGTATTGTATTAAGTGTAGTTGTTATTTTTGTCAAAGATCAATCAGATTTATCATTGTATGTATTTACTATTGCTATTGTGAC 8II05IIIIIIIIIIIIIIIFEIII82II/+III?+I?IIFI(IIIII22I6+*I+=0*%F/&7&C:?,?-*9@=3&6@1&<.1%*@%/32&,#./-('#% " + 
      "@SRR022868.8762487/2 ACGAACGAAACAATTGCCAGACGTGTATCCAATTAACCGAAACAAAGCTAATGTATCGTTTTAAATAGATAAACAAAGGTAATTGGTTTAATACCGTCACA 59IABIII6EIIAIIIII;I;EI9I:;IID-&II+9C9I%*(9+-&..;&()5'&6-:'0,/*&*((*%+%#%&&#$*&$&\")%&*%&&#%(''\")$*#&# " + 
      "@SRR022868.8762490/1 TTTATATGAAGAAATGAACCATTTATTGTATTGTAGTTCAGCTGGTCATGAGCCTGGATATATTTATCGCGCTGAAAAAGAAGACTTTGAAGAAATTTCAG III4IHIII=IIIIIIIIIIHIII>IEIIIIIIH9IDII1IIIIB:A.II$I-:CHF(8*.+834(1/@1<4+.''%#'/%%2'#+)*.#%2#%&(')(&) " + 
      "@SRR022868.8762490/2 ATTAAATCATCAAGGTATATTGGAATGTCTTGTTGTTGATATCGTGTTTGTGAACTGATTCCTAACACTCTACCTCTAACTGAAATTTCTTCACATTCTTC ,IIDI:IIIII8III+/I4IIIF,/I$IFHI97=IC6>)8&4.I6?47-1&2%(*2;&+,)(,&&*'&1-)#+%%+)$%))'$$$'%&)%'(%$%+%&$(& " + 
      "@SRR022868.8762492/1 CCAGGATCAAACTCTCCATAAAAATTATGATGTTTGATTAGCTCATAAGTACTAAATAATGTTTGTAACTTATAGTTACGTTTTTTGGAATTAACGTTGAC II;IIBIII8>IIIIII7IDIA0IIIDII6IIIIII,II9IIII4I+3I80II+'.I*1II3<>8;)+4@<+I*F.A,0F.0443*&)&#$<#(-*%+,#' " + 
      "@SRR022868.8762492/2 TGTCGGTAAGAAAAATGAACATTGAAAACTGAATGACAATATGTCAACGTTAATTCCAAAAAACGTAACTATAAGTTACAAACATTATTTAGTACTTATTT IIIIII0IHI?III49B&:852ID'4A;)56.%/B(7(%/)E.*@(+/+#0&)*/(,&&%+(,('$)((+%%$&$%+$(#&$$#$)$&*&&#$#'%&$&!# " ;
  
  // Path of flash binary
  private String flashBinaryPath = "/home/avijit/Downloads/FLASH-1.2.2/flash";  
  
  //remember to add spaces after the records - they are delimiters
  private String expectedOutput = 
      "@SRR022868.8762453 TAATTTAACTTTGTCTGATAATTGTACGCTTAAATCAACGTCTTTCGGTAAAATATCAGATTGTGCTGGAATTAATAACACATCATCAAACGTTAATGATTCTTTTGCAAATTTACTTTCCCACATTAAAAACAGCCT II0IIII8IIIIIIIII=I,IIIIIIIIIII,1+II4+HIBIIII?IIE6*(1I1;I0E-I+I9I5/IA.,3=62I6(1(I02CI5;IA#IC).II*II.4I5&*:DIIII*(+II5,&IIIIII>IIIIIIIIIIIC " +
      "@SRR022868.8762473 TTTTTATTTAAATTAATCATATAATTGCGAGGAGAATATTATGGATTTCGTTAATAATGATACAAGACAAATTGCTAAAAACTTATTAGGTGTCAAAGTGATTTATCAGGATACCACTCAAACGTATACAGGCTACATCGTGGAA IIIIIIII=@=FIIIIIIIIIIIIIIIII3II%I>/I,II=III2IIIIII:<1F/7I<I5B<42I*9;,.A,I?@<,1?5@/090>=;/3@7I44762?I&I*:&II/F9&1II)I.IIG5I4IIIIGIIIIIIIIIIIIIIII " + 
      "@SRR022868.8762490 TTTATATGAAGAAATGAACCATTTATTGTATTGTAGTTCAGCTGGTCATGAGCCTGGATATATTTATCGCGCTGAAAAAGAAGAATTTGAAGAAATTTCAGTTAGAGGTAGAGTGTTAGGAATCAGTTCACAAACACGATATCAACAACAAGACATTCCAATATACCTTGATGATTTAAT III4IHIII=IIIIIIIIIIHIII>IEIIIIIIH9IDII1IIIIB:A.II$I-:CHF(8*.+834(1/@1<4+.''%#'/(%2'#+#*.'%2&%'(')())%$)+%%+#)-1&'*&&,(),+&;2*(%2&1-74?6I.4&8)>6CI=79IHFI$I/,FIII4I/+III8IIIII:IDII, " +
      "@SRR022868.8762492 CCAGGATCAAACTCTCCATAAAAATTATGATGTTTGATTAGCTCATAAGTACTAAATAATGTTTGTAACTTATAGTTACGTTTTTTGGAATTAACGTTGACATATTGTCATTCAGTTTTCAATGTTCATTTTTCTTACCGACA II;IIBIII8>IIIIII7IDIA0IIIDII6IIIIII,II9II#!4I+3I80II+*.I*1II3<>8;++4@<+I+F.A,0F.0443*,)/*)<0(-/++@*.E)/%(7(B/%.65);A4'DI258:&B94III?IHI0IIIIII ";
  
  @Test
  public void testMap() throws IOException {
    File temp = getTempDirectory();
    File outputFile = new File(temp, "output");
    File flashInput = new File(temp, "flashInput.avro");
    runFlashTest(flashInput, outputFile);
    File outputAvroFile = new File(outputFile, "part-00000.avro");
    Schema schema = (new fastqrecord()).getSchema();
    DatumReader<fastqrecord> datum_reader =
        new SpecificDatumReader<fastqrecord>(schema);
    DataFileReader<fastqrecord> reader =
        new DataFileReader<fastqrecord>(outputAvroFile, datum_reader);
    int numberOfFastqReads = 0;
    AvroCollectorMock<fastqrecord> collector_mock = new AvroCollectorMock<fastqrecord>();
    //Read the avro records from output/part-00000.avro and collect them in mock collector
    while(reader.hasNext()){
      fastqrecord record = reader.next();
      collector_mock.collect(record);
      numberOfFastqReads++;
    }
    HashMap<String, String> expectedHashMap = getExpectedHashMap();
    // The number of records produced should be exactly same as expected
    assertEquals(expectedHashMap.size(),numberOfFastqReads);
    //The records produced should be the same as expected
    assertMapOutput(collector_mock, expectedHashMap);
  }
  
  @Test
  public void testRun() {
    File temp = getTempDirectory();
    File outputFile = new File(temp, "output");
    File outFile = new File(temp, "flashInput.avro");
    runFlashTest(outFile, outputFile);
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

  private void runFlashTest(File flashInput, File outputPath){
    writeDataToFile(flashInput);
    runApp(flashInput, outputPath);
  }
  
  /**
   * Runs InvokeFlash with a blockSize of 3
   * @param input
   * @param output
   */
  private void runApp(File input, File output){
    InvokeFlash flashInvoker = new InvokeFlash();
    InvokeFlash.blockSize = 3;
    String[] args =
      {"--inputpath=" + input.toURI().toString(),
       "--outputpath=" + output.toURI().toString(),
       "--flash_binary="+ flashBinaryPath};

    try {
      flashInvoker.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
  
  /**
   * Writes data to a file in avro format
   * @param outFile
   */
  private void writeDataToFile(File outFile){
    Schema schema = (new MatePair()).getSchema();
    DatumWriter<MatePair> datum_writer =
        new SpecificDatumWriter<MatePair>(schema);
    DataFileWriter<MatePair> writer =
        new DataFileWriter<MatePair>(datum_writer);
    StringTokenizer st = new StringTokenizer(joinedFastq, " ");
    try {
      writer.create(schema, outFile);
      while(st.hasMoreTokens()){
        MatePair mateRecord = getMateRecord(st);
        writer.append(mateRecord);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }
  }
  
  private void assertMapOutput(AvroCollectorMock<fastqrecord> collector_mock, HashMap<String, String> expectedHashMap){
    Iterator<fastqrecord> iterator = collector_mock.data.iterator();
    while (iterator.hasNext()) {
      fastqrecord flashedRecord = iterator.next();
      String id = flashedRecord.getId().toString();
      String dna = flashedRecord.getRead().toString();
      String qvalue = flashedRecord.getQvalue().toString();
      String receivedValue = dna + " " + qvalue;
      assertEquals(expectedHashMap.get(id), receivedValue);
    }
  }
  
  /**
   * Reads next 6 records separated by spaces and creates a mateRecord from them
   * @param testStringToken
   * @return
   */
  private MatePair getMateRecord(StringTokenizer testStringToken){
    MatePair mateRecord = new MatePair();
    mateRecord.setLeft(new fastqrecord());
    mateRecord.setRight(new fastqrecord());
    mateRecord.getLeft().setId(testStringToken.nextToken());
    mateRecord.getLeft().setRead(testStringToken.nextToken());
    mateRecord.getLeft().setQvalue(testStringToken.nextToken());
    mateRecord.getRight().setId(testStringToken.nextToken());
    mateRecord.getRight().setRead(testStringToken.nextToken());
    mateRecord.getRight().setQvalue(testStringToken.nextToken());
    return mateRecord;
  }

  private HashMap<String, String> getExpectedHashMap(){
    HashMap<String, String> expectedHashMap = new HashMap<String, String>();
    StringTokenizer tokenizer = new StringTokenizer(expectedOutput, " ");
    while(tokenizer.hasMoreTokens()){
      String seqId = tokenizer.nextToken();
      String dna = tokenizer.nextToken();
      String qvalue = tokenizer.nextToken();
      String expectedString = dna +" " + qvalue;
      expectedHashMap.put(seqId, expectedString);
    }
    return expectedHashMap;
  }

}
