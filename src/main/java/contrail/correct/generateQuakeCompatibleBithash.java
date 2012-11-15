/*
 * This file should be deprecated. Not for use now after Bithash Generation is done in
 * FilterGlobalCountFile
 * 
 */
package contrail.correct;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.GenericOptionsParser;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;


public class generateQuakeCompatibleBithash extends Stage{

	private void parse(String args[])
	{
		 parseCommandLine(args);
	}
	
	protected Map<String, ParameterDefinition> createParameterDefinitions() {
	     HashMap<String, ParameterDefinition> defs =
	         new HashMap<String, ParameterDefinition>();

	     defs.putAll(super.createParameterDefinitions());
	     
	     ParameterDefinition QuakeHome = new ParameterDefinition("QuakeHome", "any" +
	    		"The folder that contains the bin folder of quake", String.class, new String(""));
	   
	     ParameterDefinition tempWritableFolder = new ParameterDefinition("tempWritableFolder", "any" +
		    		"The folder that is writable provided on every node in the" +
		    		"cluster with write permissions", String.class, new String(""));
	     
	     ParameterDefinition QuakeTempFolderName = new ParameterDefinition("QuakeTempFolderName", "any" +
		    		"Flash NFS directory path", String.class, new String(""));
	     
	     for (ParameterDefinition def: new ParameterDefinition[] {QuakeHome, tempWritableFolder, QuakeTempFolderName, tempWritableFolder}) {
	    	      defs.put(def.getName(), def);
	     }
	    
	     for (ParameterDefinition def:
	       ContrailParameters.getInputOutputPathOptions()) {
	       defs.put(def.getName(), def);
	     }
	     return Collections.unmodifiableMap(defs);
	   }
	
	public void generateCompatibleBithash()
	{	
		Process p;
		String tempWritableFolder = (String) stage_options.get("tempWritableFolder");
		String QuakeLocalFolderName = (String) stage_options.get("QuakeTempFolderName");
		String QuakeHome = (String)stage_options.get("QuakeHome");
		String quakeTempLocalPath = tempWritableFolder+"/"+QuakeLocalFolderName;
		String globalBithashLocation = (String) stage_options.get("inputpath");
		try {
			//Cleaning up older data
			File f = new File(quakeTempLocalPath+"/merged.out");
			if(f.exists()) f.delete();
			f = new File(quakeTempLocalPath+"/qcb");
			if(f.exists()) f.delete();
			
			String command = "hadoop dfs -getmerge "+globalBithashLocation+" "+quakeTempLocalPath+"/merged.out";
			System.out.println(command);
			p = Runtime.getRuntime().exec(command);
			int i = p.waitFor();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		String writer_source = QuakeHome+"/src/write_bithash";

		String command = writer_source+" "+quakeTempLocalPath+"/merged.out "+quakeTempLocalPath+"/qcb";
		System.out.println(command);
		try {
			 
			p = Runtime.getRuntime().exec(command);
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//output in bithashquake.out
		
	}
	
	public static void main(String[] args){
		   try 
		      {   
		    	  	 GenericOptionsParser parser = new GenericOptionsParser(args);
			    	 String[] toolArgs = parser.getRemainingArgs();
			    	 generateQuakeCompatibleBithash bithashObj = new generateQuakeCompatibleBithash();
			    	 bithashObj.parse(toolArgs);
			    	 bithashObj.generateCompatibleBithash();
			    	// System.out.print("Cutoff: " + cutoffObj.getCutoff());
		      } 
		      catch (Exception e) {
		         e.printStackTrace();
		        }
	 }
}
