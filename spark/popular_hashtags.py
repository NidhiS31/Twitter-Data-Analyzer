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

cluster_name = "spark-cluster" 
bucket_name = "twitter_bucket"

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



df_tumbling_window = df_streaming_visits \
                .where("pid is null") \
                .withWatermark("date_time","10 minutes") \
                .groupBy(       
                    window(df_streaming_visits.date_time,"180 seconds"),           
                    df_streaming_visits.type,
                    df_streaming_visits.hashtags,
                    df_streaming_visits.state) \
                .count()

def foreach_batch_function(df,epoch_id) :

    df_final = df.selectExpr("window.start as window_start_time","window.end as window_end_time",
                        "type","hashtags","state","count")

    df_final.show(10,False)

    df_final.coalesce(1).write \
    .format("avro") \
    .mode("append") \
    .option("checkpointLocation","twitter_bucket/spark_checkpoints/") \
    .option("path","twitter_bucket/tweets_by_hashtags/") \
    .save()

query = (

        df_tumbling_window.writeStream.trigger(processingTime="75 seconds") \
        .foreachBatch(foreach_batch_function)
        .outputMode("complete")
        .start()
    )


query.awaitTermination()
