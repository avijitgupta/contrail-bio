package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;

import contrail.stages.ContrailParameters;
import contrail.stages.PairMarkAvro;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;


/*Takes in input in form of avro data from two input directories 
 * (flash mate 1 and flash mate2 or quake mate 1 and quake mate 2) 
 * and emits out the joined record according to the schema specified.
 * This is an embedded pig join script.
 *
 * You can run this stage by specifying parameters in:
 *  --inputpath1=path/to/mate/1 --inputpath2=path/to/mate/2 --outputpath=path/to/store/joined/files
 */


public class pigAvroRecordJoin extends Stage {

	   PigServer pigServer;
	   
	   private void parse(String args[])
	   {
			 parseCommandLine(args);
	   }
	   
	   private PigServer getPigServer()
	   {
		     PigServer pigServer = null;
	    	 try 
	    	 {
	    		 /*
	    		  * TODO:  These paths are generally present for a system with pig installed.
	    		  * We can link this with the path at which maven downloads its Jars. We can put in
	    		  * all required jars in pom.xml
	    		  * What is the best way to do that?
	    		  * you can get pig by following steps on 
	    		  * https://ccp.cloudera.com/display/CDHDOC/Pig+Installation
	    		  */
	    		 
	    		 pigServer = new PigServer(ExecType.MAPREDUCE);	 
				 pigServer.registerJar("/usr/lib/pig/contrib/piggybank/java/piggybank.jar");
		    	 pigServer.registerJar("/usr/lib/pig/contrib/piggybank/java/lib/avro-1.5.4.jar");
		    	 pigServer.registerJar("/usr/lib/pig/contrib/piggybank/java/lib/jackson-core-asl-1.7.3.jar");
		    	 pigServer.registerJar("/usr/lib/pig/contrib/piggybank/java/lib/jackson-mapper-asl-1.7.3.jar");
		    	 pigServer.registerJar("/usr/lib/pig/contrib/piggybank/java/lib/json-simple-1.1.jar");
		    	 pigServer.registerJar("/usr/lib/pig/contrib/piggybank/java/lib/snappy-java-1.0.3.2.jar");
	    	 } catch (IOException e) {
					e.printStackTrace();
				}
	    	 return pigServer;
	   }
	   
	   public static void main(String[] args){
		   PigServer pigServer = null;  
		   try 
		      {   
			   		 //For command line arguments
			    	 GenericOptionsParser parser = new GenericOptionsParser(args);
			    	 String[] toolArgs = parser.getRemainingArgs();
			    	 pigAvroRecordJoin pigJoinObj = new pigAvroRecordJoin();
			    	 pigJoinObj.parse(toolArgs);
			    	 pigServer = pigJoinObj.getPigServer();
			    	 if(pigServer!=null)
			    		 pigJoinObj.runJoinQuery(pigServer);
			    	 else
			    		 throw new Exception("ERROR: Pig Server Creation Unsuccessful");
		      } 
		      catch (Exception e) {
		         e.printStackTrace();
		        }
		   }
			
	   
	   
		   public void runJoinQuery(PigServer pigServer) throws IOException {
			   
			   //the input path for Paired Reads 1 
		       String inputFile1 = (String) stage_options.get("inputpath1");
		     
		       //the input path for Paired Reads 2 
		       String inputFile2 = (String) stage_options.get("inputpath2");
		       
		       //the output path for joined reads 
		       String outputPath = (String) stage_options.get("outputpath");
			   
		       //Pig queries
		       pigServer.registerQuery("fq1 = load '" + inputFile1 + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage();");
		       pigServer.registerQuery("fq2 = load '" + inputFile2 + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage();");
		       pigServer.registerQuery("fq1_filtered =  FOREACH fq1 GENERATE $0, $1, $2;");
		       pigServer.registerQuery("fq2_filtered =  FOREACH fq2 GENERATE $0, $1, $2;");
		       pigServer.registerQuery("joinedfq = JOIN fq1_filtered BY $0, fq2_filtered BY $0;");
		       
		       
		       /*TODO: Schema is currently specified within the pig query. We might want to use a schema
		        * mentioned in contrail.avpr eventually with this. What might be a good way to do
		        * that? 
		        */
		       String storeQuery = "STORE joinedfq INTO '"+ outputPath +"' USING  org.apache.pig.piggybank.storage.avro.AvroStorage("+
				      	" '{\"schema\":{\"type\":\"record\",\"name\":\"joinedfqrecord\", " + 
				      	"\"doc\":\"Foined FastQ File Structure\","+
						"\"fields\": [{ \"name\":\"id1\",\"type\":\"string\"},"+
						" { \"name\":\"read1\", \"type\":\"string\"},"+
						" { \"name\":\"qvalue1\", \"type\":\"string\"},"+
						"{ \"name\":\"id2\",\"type\":\"string\"},"+
						"{ \"name\":\"read2\", \"type\":\"string\"},"+
						"{ \"name\":\"qvalue2\", \"type\":\"string\"}]}"+
						"}');";
		    /*   String storeQuery = "STORE joinedfq INTO '"+ outputPath +"' USING  org.apache.pig.piggybank.storage.avro.AvroStorage("+
				      	" '{\"schema\":{\"type\":\"record\",\"name\":\"materecord\", " + 
				      	"\"doc\":\"Mate File Record Structure\""+
						"\"fields\": [{\"name\":\"mate1\",\"type\":\"contrail.correct.fastqrecord\"},"+
						"{\"name\":\"mate2\",\"type\":\"contrail.correct.fastqrecord\" }]}"+
						"}');";*/
		      pigServer.registerQuery(storeQuery);
		   
		   }
		   
		   /*
		    * Get the options required by this stage.
		    */
		   protected Map<String, ParameterDefinition> createParameterDefinitions() {
		     HashMap<String, ParameterDefinition> defs =
		         new HashMap<String, ParameterDefinition>();

		     defs.putAll(super.createParameterDefinitions());
		     
		     ParameterDefinition inputpath1 = new ParameterDefinition("inputpath1", "any" +
		    		"path for files from input 1", String.class, new String(""));
		     
		     ParameterDefinition inputpath2 = new ParameterDefinition("inputpath2", "any" +
			    		"path for files from input 2", String.class, new String(""));
		     
		     for (ParameterDefinition def: new ParameterDefinition[] {inputpath1, inputpath2}) {
		    	      defs.put(def.getName(), def);
		     }
		    
		     for (ParameterDefinition def:
		       ContrailParameters.getInputOutputPathOptions()) {
		       defs.put(def.getName(), def);
		     }
		     return Collections.unmodifiableMap(defs);
		   }
		}
	

