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
      .option("kafka.bootstrap.servers", cluster_name+"-w-1:9092") \
      .option("subscribe", "user_tweet_logs") \
      .option("failOnDataLoss","false") \
      .load() \
      .selectExpr("CAST(value AS STRING) ")

schema = StructType(
    [
        StructField('hashtags', StringType(), True),
        StructField('date_time', TimestampType(), True),
        StructField('type', StringType(), True),
        StructField('pid', IntegerType(), True),
	StructField('mentions', StringType(), True),
   ]
)

df_parsed = df.select("value")

df_visits = df_parsed.withColumn("data", from_json("value",schema)).select(col('data.*'))

df_tumbling_window = df_visits \
    .where("pid is not null") \
    .withWatermark("date_time", "10 minutes") \
    .groupBy(
        df_visits.mentions,
        df_visits.type) \
    .count()

""" For Testing / Debugging """

# query = df_windowed_counts.writeStream \
#             .outputMode("complete") \
#             .format("console") \
#             .start()

def foreach_batch_function(df,epoch_id) :
    print(epoch_id)
    print("*******************")
    df.show()
    
    df.coalesce(1).write \
    .format("avro") \
    .mode("append") \
    .option("checkpointLocation",bucket_name+"/aggregator-checkpoints/") \
    .option("path",bucket_name+"/aggregations/") \
    .save()
        
query = df_tumbling_window.writeStream.trigger(processingTime="10 seconds") \
          .outputMode("complete") \
          .format("console") \
          .start()

query.awaitTermination()