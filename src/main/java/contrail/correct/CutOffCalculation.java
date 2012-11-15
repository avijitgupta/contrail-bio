package contrail.correct;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.FileHelper;

/*
 * This class calculates the cutoff by invoking cov_model.py - Quake
 */
public class CutOffCalculation extends Stage {
  int cutoff;
  private static final Logger sLogger = Logger.getLogger(CutOffCalculation.class);
  public int getCutoff() throws Exception{
    if(cutoff!=0) return cutoff;
	else throw new Exception("ERROR: Cutoff not calculated");
  }
	
  private void parse(String args[]){
	parseCommandLine(args);
  }
  
  /* For calculating the cutoff, we need a part file of the Kmer counter
   * to be brought into the directory of quake, which is running on a single node.
   * A single part file is enough for calculating the cutoff.
   * The part file is brought from the HDFS onto the local system where cov_model.py is
   * run on it to calculate the cutoff value. The value is read from the output stream of the 
   * cove_model.py process.
   */
  public void calculateCutoff() throws Exception{
    //inputPath is the path of the file on DFS where the non avro count part is stored
    String inputPath = (String) stage_options.get("inputpath");
    String covModelPath = (String) stage_options.get("cov_model");
    //String Quake_Path = ContrailConfig.Quake_Home+"/";
    String s;
    String tempWritableFolder = FileHelper.createLocalTempDir().getAbsolutePath();
    String QuakeTempFolderName = "countPartFolder";
    String QuakeTempFolder = new File(tempWritableFolder,QuakeTempFolderName).getAbsolutePath();
    File f = new File(QuakeTempFolder, "part-00000");
    if (f.exists()){
      f.delete();
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path fp1 = new Path(inputPath);
    Path fp2 = new Path(QuakeTempFolder, "part-00000");
    File tempDirectory = new File(QuakeTempFolder);
    if(!tempDirectory.exists()){
      tempDirectory.mkdir();
    }
    fs.copyToLocalFile(fp1,fp2);
    StringTokenizer str ;
    //command to run cov_model.py
    String command = covModelPath+" --int "+QuakeTempFolder+"/part-00000";
    sLogger.info("Cutoff Calculation: "+ command);
    Process p = Runtime.getRuntime().exec(command);
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
    sLogger.info("Running cov_model.py\n");
    while ((s = stdInput.readLine()) != null) {
      str = new StringTokenizer(s);
      /*This is a hack - everything displayed by the execution of 
       * cov_model.py here is stored in str line by line. In the end, the token containing
       * the cutoff is taken out - What is a better way of doing this?
       */
      if(str.countTokens()!=0 && str.nextToken().trim().equals("Cutoff:")){
        String ss = str.nextToken();
        cutoff = Integer.parseInt(ss);
        break;
      }
   }
   p.waitFor();
   File tempFile = new File(tempWritableFolder);
   if(tempFile.exists()){
     FileUtils.deleteDirectory(tempFile);
   }
}
	
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition QuakeHome = new ParameterDefinition("cov_model", "any" +
	    		            "location of cov_model.py", String.class, new String(""));
    for (ParameterDefinition def: new ParameterDefinition[] {QuakeHome}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public static void main(String[] args){
    try {   
      GenericOptionsParser parser = new GenericOptionsParser(args);
      String[] toolArgs = parser.getRemainingArgs();
      CutOffCalculation cutoffObj = new CutOffCalculation();
      cutoffObj.parse(toolArgs);
      cutoffObj.calculateCutoff();
      sLogger.info("Cutoff: " + cutoffObj.getCutoff());
    } 
    catch (Exception e) {
      e.printStackTrace();
    }
  }	
}
