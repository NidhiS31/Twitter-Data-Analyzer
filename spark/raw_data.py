from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time 
import datetime
conf = SparkConf(). \
setAppName("Data Streamer"). \
setMaster("yarn-client")
sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)

cluster_name = "gs://spark-cluster"

bucket_name = "gs://twitter_bucket"

spark = SparkSession \
    .builder \
    .appName("twitter-user-logs-streaming") \
    .getOrCreate()

df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "spark-cluster-w-1:9092") \
      .option("subscribe", "user_tweet_logs") \
      .option("failOnDataLoss","false") \
      .load() \
      .selectExpr("CAST(value AS STRING) ")

schema = StructType(
    [
        StructField('hashtag', StringType(), True),
        StructField('date_time', TimestampType(), True),
        StructField('pid', IntegerType(), True),
        StructField('state', StringType(), True),
        StructField('ip_address', StringType(), True)
   ]
)

df_parsed = df.select("value")

df_streaming_visits = df_parsed.withColumn("data", from_json("value",schema)).select(col('data.*'))

def foreach_batch_function(df,epoch_id) :
    df.coalesce(1).write \
    .format("parquet") \
    .mode("append") \
    .option("checkpointLocation", "twitter_bucket/spark_checkpoints") \
    .option("path", "twitter_bucket/streaming_output") \
    .save()

query = df_streaming_visits.writeStream \
            .trigger(processingTime="60 seconds") \
            .foreachBatch(foreach_batch_function) \
            .outputMode("append") \
            .start()

query.awaitTermination()