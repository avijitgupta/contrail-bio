package contrail.correct;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class runFlash {
/*
 * This invokes the flash executable within various cluster nodes where mappers 
 * have created local files. This method is invoked during the close() method of 
 * the mapper that creates local files. 
 */
public static void flashRunner(String filePathFq1, String filePathFq2, String localTime, 
								String FlashFinalOut, String FlashLocalOut, String FlashHome){
  String flashPath = FlashHome;
  String FlashOutputDirectory = FlashLocalOut +"/"+ localTime;
  String command = flashPath+"/flash "+filePathFq1+" "+filePathFq2+" -d "+ FlashOutputDirectory;
  System.out.println(command);
  try {
    Process p = Runtime.getRuntime().exec(command);
	BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	String s;
	System.out.println("Output of Flash Run");
	while ((s = stdInput.readLine()) != null) {
		System.out.println(s); 
	}
	p.waitFor();	
  } catch (Exception e) {
	  e.printStackTrace();
  }
  //These are the three output files of Flash on each machine.. The Combined File, and Two Not Combined Files..
  // Here, the Not Combined Files are discarded - not used further. 
  String combinedFilePath = FlashOutputDirectory + "/out.extendedFrags.fastq";		
  String  flash_hdfs_out;
  // The Output of Flash is written to FlashFinalOut, for record.
  flash_hdfs_out = FlashFinalOut + "/out.extendedFrags"+localTime+".fastq";
  try{
 	Configuration conf2 = new Configuration();
	FileSystem fs = FileSystem.get(conf2);
	//Deleting output path if exists
	Path fp1 = new Path(combinedFilePath);
	if(!fs.exists(new Path(FlashFinalOut))){
		fs.mkdirs(new Path(FlashFinalOut));			    	
	}
	Path fp2 = new Path(flash_hdfs_out);
	fs.copyFromLocalFile(fp1,fp2);
	//   fs.moveFromLocalFile(fp1,fp3);
    }
  catch(Exception e){
	  System.out.println(e.toString());
   }
  // CorrectUtil.deleteDir(new File(FlashOutputDirectory));
   }

	
}
