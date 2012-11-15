mvn clean
mvn package -DskipTests=true
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.KmerCounter --inputpath=ContrailPlus/Singles_Avro --outputpath=ContrailPlus/Kmer_Count_New --K=13
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.FilterGlobalCountFile --inputpath=ContrailPlus/KmerCountFile --outputpath=ContrailPlus/Q_Bithashes --cutoff=3 --K=13
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.pigAvroRecordJoin --inputpath1=ContrailPlus/Flash_Mate_1_Avro --inputpath2=ContrailPlus/Flash_Mate_2_Avro --outputpath=ContrailPlus/FlashJoinOut
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.FastQtoAvro --inputpath=ContrailPlus/Flash_Mate_2 --outputpath=ContrailPlus/Flash_Mate_2_Avro
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.InvokeFlash --inputpath=ContrailPlus/FlashJoinOut --tempWritableFolder=/home/avijit/jeremy/contrail-bio/work --outputpath=ContrailPlus/Flash_Out_Joined --flashHDFS=binaries/flash
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.InvokeQuake --inputpath=ContrailPlus/Quake_Join_Out --tempWritableFolder=/home/avijit/jeremy/contrail-bio/work --outputpath=ContrailPlus/Quake_Corrected --quakeHDFS=binaries/correct --K=13 --bithashpath=ContrailPlus/QuakeBithash/bithash
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.InvokeQuakeForSingles --inputpath=ContrailPlus/Singles_Avro --tempWritableFolder=/home/avijit/jeremy/contrail-bio/work --outputpath=ContrailPlus/Quake_Singles_Corrected --quakeHDFS=binaries/correct --K=13 --bithashpath=ContrailPlus/QuakeBithash/bithash
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.InvokeFlash --inputpath=ContrailPlus/Flash_New_Schema_Out --outputpath=ContrailPlus/Flash_Out_Joined --flash_binary=binaries/flash
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.pigAvroRecordJoin --inputpath1=ContrailPlus/Flash_Mate_1_Avro --inputpath2=ContrailPlus/Flash_mate_2_Avro --outputpath=ContrailPlus/Flash_New_Schema_Out
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.convertToNewMateSchema --inputpath=ContrailPlus/Quake_Join_Out --outputpath=ContrailPlus/Quake_New_Schema_Out
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.ConvertFileToNonAvro --inputpath=ContrailPlus/KmerCountFile/part-00000.avro --outputpath=ContrailPlus/Kmer_Count_New
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.CutOffCalculation --inputpath=ContrailPlus/Kmer_Count_New/part-00000 --cov_model=/home/avijit/contrail-avrotest/quake/bin/cov_model.py
#hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.InvokeQuakeForMatePairs --inputpath=ContrailPlus/Quake_New_Schema_Out --outputpath=ContrailPlus/Quake_Mate_Corrected --quake_binary=binaries/correct --bithashpath=ContrailPlus/QuakeBithash/bithash --K=13
hadoop jar target/contrail-1.0-SNAPSHOT-job.jar contrail.correct.InvokeQuakeForSingles --inputpath=ContrailPlus/Singles_Avro --outputpath=ContrailPlus/Quake_Singles_Corrected --quake_binary=binaries/correct --bithashpath=ContrailPlus/QuakeBithash/bithash --K=13
