mvn clean
mvn package -DskipTests=true
#hadoop jar target/contrail-1.0-SNAPSHOT-tests-job.jar contrail.correct.FlattenTextFastQ --inputpath=ContrailPlus/Flash_Serial_Out --outputpath=ContrailPlus/Flash_Serial_Flat
hadoop jar target/contrail-1.0-SNAPSHOT-tests-job.jar contrail.correct.convertAvroFlashOutToFlatFastQ --inputpath=ContrailPlus/Flash_Out_Joined --outputpath=ContrailPlus/Flash_Avro_Flat
