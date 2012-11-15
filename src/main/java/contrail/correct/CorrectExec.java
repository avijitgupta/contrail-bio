package contrail.correct;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/*
 * This runs correct on local cluster nodes, given that local fastq files exist.
 * This is on files of size blockSize
 */
public class CorrectExec {
  
  /**
   * Runs quake correction on a given mate pair files on the local FS
   * @param filePathFq1: Path of mate 1
   * @param filePathFq2: Path of mate 2
   * @param localTime: timestamp - used as the name of the local
   * @param K : Kmer length
   * @param CorrectLocation : Location of correction binary on Distributed cache
   * @param QuakeLocalInputFolderPath : Quake temporary folder  
   * @param bitHashLocation : Location of bithash on distributed cache
   */
  public static void correctRunner(String filePathFq1, String filePathFq2, String localTime, long K, String CorrectLocation, String QuakeLocalInputFolderPath, String bitHashLocation){
    String fastqListLocation = QuakeLocalInputFolderPath+"/"+localTime+".txt";
	String data = filePathFq1+" "+filePathFq2+"\n";
	writeStringToFile(data, fastqListLocation);
	//Correction command
	String command = CorrectLocation+" -f "+ fastqListLocation + " -k " + K + " -b "+bitHashLocation;
	//CorrectUtil.executeCommand(command);	 -- remove comments
    File fp = new File(fastqListLocation);
    if(fp.exists())fp.delete();	
  }
  
  public static void correctRunner(String fastqLocation, String localTime, long K , String CorrectLocation, String QuakeLocalInputFolderPath, String bitHashLocation){
    String fastqListLocation = QuakeLocalInputFolderPath+"/"+localTime+".txt";
    writeStringToFile(fastqLocation, fastqListLocation);
    //Correction command
    String command=CorrectLocation+" -f "+ fastqListLocation + " -k " + K + " -b "+bitHashLocation;
    //CorrectUtil.executeCommand(command);	--remove comments
    File fp = new File(fastqListLocation);  
    if(fp.exists())fp.delete();	
  } 
  
  private static void writeStringToFile(String data, String filePath){
    try{
			/* Writes the temporary file to be created - its name to the temp .txt file - fastqListLocation*/
      FileWriter fstream = new FileWriter(filePath,true);
      BufferedWriter out = new BufferedWriter(fstream);
      out.write(data+"\n");
      out.close();
      fstream.close();
    }
    catch (Exception e)	{
      e.printStackTrace();
    }
  }
}
