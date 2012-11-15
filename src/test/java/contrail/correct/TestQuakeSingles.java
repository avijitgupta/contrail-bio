package contrail.correct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.commons.io.FileUtils;

public class TestQuakeSingles {
  
  private String quakeBinaryPath = "";  
  private String bithashPath = "";
  private byte[] numericBithash = {-1, -1, -5, -1, -1, -33, -1, -1};
  private int K = 3;

//5 records - added a \ at 27:212, 22:239 & 27:226 because of presence of "
  private String inputFastq = 
      "@SRR022868.8762453 AGGCTGTTTTTAATGTGGGAAAGTAAATTTGCAAAAGAATCATTAACGTTTGATGATGTGTTATTAATTCCAGCACAATCTGATATTTTACCGAAAGACGT CIIIIIIIIIII>IIIIII&,5II+(*IIIID:*&5I4.II*II.)CI5AI;5IC+0I%1(6&26&-,.)+#2.*3)%-.0-$'%&'&)%)&,$##&#+'+ " + 
      "@SRR022868.8762473 TTCCACGATGTAGCCTGTATACGTTTGAGTGGTATCCTGATAAATCACTTTGACACCTAATAAGTTTTTAGCAATTTGTCTTGTATCATTATTAACGAAAT IIIIIIIIIIIIIIIIGIIII4I5GII.I)II1&9F/II&:*I&I62(744I723.3=>/90&@5?1,<&1-,'.,/,&-2*+.&&+%/)(--$#(,%##* " + 
      "@SRR022868.8762487 ACGAACGAAACAATTGCCAGACGTGTATCCAATTAACCGAAACAAAGCTAATGTATCGTTTTAAATAGATAAACAAAGGTAATTGGTTTAATACCGTCACA 59IABIII6EIIAIIIII;I;EI9I:;IID-&II+9C9I%*(9+-&..;&()5'&6-:'0,/*&*((*%+%#%&&#$*&$&\")%&*%&&#%(''\")$*#&# " + 
      "@SRR022868.8762490 ATTAAATCATCAAGGTATATTGGAATGTCTTGTTGTTGATATCGTGTTTGTGAACTGATTCCTAACACTCTACCTCTAACTGAAATTTCTTCACATTCTTC ,IIDI:IIIII8III+/I4IIIF,/I$IFHI97=IC6>)8&4.I6?47-1&2%(*2;&+,)(,&&*'&1-)#+%%+)$%))'$$$'%&)%'(%$%+%&$(& " + 
      "@SRR022868.8762492 TGTCGGTAAGAAAAATGAACATTGAAAACTGAATGACAATATGTCAACGTTAATTCCAAAAAACGTAACTATAAGTTACAAACATTATTTAGTACTTATTT IIIIII0IHI?III49B&:852ID'4A;)56.%/B(7(%/)E.*@(+/+#0&)*/(,&&%+(,('$)((+%%$&$%+$(#&$$#$)$&*&&#$#'%&$&!# " ;
  
  private String correctedReads = 
      "@SRR022868.8762473|TTCCACGATGTAGCCTGTATACGTTTGAGTGGTATCCTGATAAATCACTTTGACACCTAATAAGTTTTTAGCAATTTGTCTTGTATCATTATTAACGAAAT|IIIIIIIIIIIIIIIIGIIII4I5GII.I)II1&9F/II&:*I&I62(744I723.3=>/90&@5?1,<&1-,'.,/,&-2*+.&&+%/)(--$#(,%##*|" +
      "@SRR022868.8762487 trim=1|ACGAACGAAACAATTGCCAGACGTGTATCCAATTAACCGAAACAAAGCTAATGTATCGTTTTAAATAGATAAACAAAGGTAATTGGTTTAATACCGTCAC|59IABIII6EIIAIIIII;I;EI9I:;IID-&II+9C9I%*(9+-&..;&()5'&6-:'0,/*&*((*%+%#%&&#$*&$&\")%&*%&&#%(''\")$*#&|" +
      "@SRR022868.8762490|ATTAAATCATCAAGGTATATTGGAATGTCTTGTTGTTGATATCGTGTTTGTGAACTGATTCCTAACACTCTACCTCTAACTGAAATTTCTTCACATTCTTC|,IIDI:IIIII8III+/I4IIIF,/I$IFHI97=IC6>)8&4.I6?47-1&2%(*2;&+,)(,&&*'&1-)#+%%+)$%))'$$$'%&)%'(%$%+%&$(&|" +
      "@SRR022868.8762492 trim=2|TGTCGGTAAGAAAAATGAACATTGAAAACTGAATGACAATATGTCAACGTTAATTCCAAAAAACGTAACTATAAGTTACAAACATTATTTAGTACTTAT|IIIIII0IHI?III49B&:852ID'4A;)56.%/B(7(%/)E.*@(+/+#0&)*/(,&&%+(,('$)((+%%$&$%+$(#&$$#$)$&*&&#$#'%&$&|" ;

  public void testMap() throws IOException {
    File temp = getTempDirectory();
    File outputFile = new File(temp, "output");
    File flashInput = new File(temp, "quakeInput.avro");
    runQuakeTest(temp, flashInput, outputFile);
    File outputAvroFile = new File(outputFile, "part-00000.avro");
    Schema schema = (new fastqrecord()).getSchema();
    DatumReader<fastqrecord> datum_reader =
        new SpecificDatumReader<fastqrecord>(schema);
    DataFileReader<fastqrecord> reader =
        new DataFileReader<fastqrecord>(outputAvroFile, datum_reader);
    int numberOfFastqReads = 0;
    ArrayList<fastqrecord> output = new ArrayList<fastqrecord>();
    while(reader.hasNext()){
      fastqrecord record = reader.next();
      output.add(record);
      numberOfFastqReads++;
    }
    HashMap<String, String> expectedHashMap = getExpectedHashMap();
    assertEquals(expectedHashMap.size(),numberOfFastqReads);
    assertMapOutput(output, expectedHashMap);
    if(temp.exists()){
      FileUtils.deleteDirectory(temp);
    }
  }
  
  private void assertMapOutput(ArrayList<fastqrecord> actualOutput, HashMap<String, String> expectedHashMap){
    Iterator<fastqrecord> iterator = actualOutput.iterator();
    while (iterator.hasNext()) {
      fastqrecord flashedRecord = iterator.next();
      String id = flashedRecord.getId().toString();
      String dna = flashedRecord.getRead().toString();
      String qvalue = flashedRecord.getQvalue().toString();
      String receivedValue = dna + " " + qvalue;
      assertEquals(expectedHashMap.get(id), receivedValue);
    }
  }
  
  private HashMap<String, String> getExpectedHashMap(){
    HashMap<String, String> expectedHashMap = new HashMap<String, String>();
    StringTokenizer tokenizer = new StringTokenizer(correctedReads, "|");
    while(tokenizer.hasMoreTokens()){
      String seqId = tokenizer.nextToken();
      String dna = tokenizer.nextToken();
      String qvalue = tokenizer.nextToken();
      String expectedString = dna +" " + qvalue;
      expectedHashMap.put(seqId, expectedString);
    }
    return expectedHashMap;
  }
  
  public void testRun() throws IOException {
    File temp = getTempDirectory();
    File outputFile = new File(temp, "output");
    File outFile = new File(temp, "quakeInput.avro");
    runQuakeTest(temp, outFile, outputFile);
    if(temp.exists()){
      FileUtils.deleteDirectory(temp);
    }
  }
  
  private void runQuakeTest(File tempDirectory, File quakeInput, File outputPath) throws IOException{
    writeDataToFile(quakeInput);
    runApp(quakeInput, outputPath);
  }
  
  private void runApp(File input, File output) throws IOException{
    InvokeQuakeForSingles quakeInvoker = new InvokeQuakeForSingles();
    InvokeQuakeForSingles.blockSize = 3;
    String bitHashDir = getTempDirectory().getAbsolutePath();
    //Write the bithash to a temporary file within this bithashDir
    bithashPath = new File(bitHashDir, "bithash").getAbsolutePath();
    writeBithash(bithashPath);
    String[] args =
      {"--inputpath=" + input.toURI().toString(),
       "--outputpath=" + output.toURI().toString(),
       "--quake_binary="+ quakeBinaryPath,
       "--bithashpath="+ bithashPath,
       "--K=" +K };

    try {
      quakeInvoker.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
    //cleanup
    File temp = new File(bitHashDir);
    if(temp.exists()){
      FileUtils.deleteDirectory(temp);
    }
  }
  
  private void writeBithash(String bithashFilePath) throws IOException {
    File file = new File(bithashFilePath);
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(numericBithash);
    outputStream.close();
  }
  
  private void writeDataToFile(File outFile){
 // Write the data to the file.
    Schema schema = (new fastqrecord()).getSchema();
    DatumWriter<fastqrecord> datum_writer =
        new SpecificDatumWriter<fastqrecord>(schema);
    DataFileWriter<fastqrecord> writer =
        new DataFileWriter<fastqrecord>(datum_writer);
    StringTokenizer st = new StringTokenizer(inputFastq, " ");
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
  
  public static void main(String[] args) throws IOException{
    if(args.length!=1 || !args[0].contains("--quake_binary=")){
      throw new IOException("Specify --quake_binary parameter only\nArgument Example: --quake_binary=/path/to/flash/binary");
    }
    TestQuakeSingles tester = new TestQuakeSingles();
    tester.quakeBinaryPath = args[0].substring(args[0].indexOf('=')+1);
    if(tester.quakeBinaryPath.trim().length() == 0){
      throw new IOException("Specify --quake_binary parameter only\nArgument Example: --quake_binary=/path/to/flash/binary");
    }
    tester.testRun();
    System.out.println("Execution Test PASSED");
    tester.testMap();
    System.out.println("Correctness Test PASSED");
  } 
}
