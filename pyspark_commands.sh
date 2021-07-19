gcloud dataproc jobs submit pyspark \
spark-etl/data-streaming.py \
--cluster=spark-cluster \
--region=us-east1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,jar-files/spark-sql-kafka-0-10_2.11-2.4.2.jar

gcloud dataproc jobs submit pyspark spark-etl/popular_hashtags.py \
--cluster=spark-cluster \
--region=us-east1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,jar-files/spark-sql-kafka-0-10_2.11-2.4.2.jar \
--properties spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.2


gcloud dataproc jobs submit pyspark spark-cluster/data_streaming.py \
--cluster=sparksql-cluster\
--region=asia-east1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,jar-files/spark-sql-kafka-0-10_2.11-2.4.2.jar 


gcloud dataproc jobs submit pyspark streaming_aggr.py \
--cluster=spark-cluster \
--region=us-east1 \
--jars=spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,spark-sql-kafka-0-10_2.11-2.4.2.jar

gcloud dataproc jobs submit pyspark stream-logs.py \
--cluster=spark-cluster \
--region=us-east1 \
--jars=spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,spark-sql-kafka-0-10_2.11-2.4.2.jar