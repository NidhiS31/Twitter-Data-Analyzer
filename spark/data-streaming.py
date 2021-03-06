from pyspark import SparkConf,SparkContext 
from pyspark.sql import SparkSession,SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import * 

import time 
import datetime 

conf = SparkConf(). \
setAppName("Data Streamer"). \
setMaster("yarn-client")


sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)

spark = SparkSession \
        .builder \
        .appName("twitter-user-logs-streaming") \
        .getOrCreate()



df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","spark-etl-w-1:9092") \
    .option("subscribe","twitter-user-logs-streaming") \
    .load() \
    .selectExpr("CAST(value as STRING)")


schema = StructType(
  [
      StructField('hashtag',StringType(),True),
      StructField('date_time',TimestampType(),True),
      StructField('type',StringType(),True),
      StructField('pid',IntegerType(),True),
      StructField('state',StringType(),True),
      StructField('ip_address',StringType(),True)
  ]
)


df_parsed = df.select("value")

df_streaming_visits = df_parsed.withColumn("data",from_json("value",schema)).select(col('data.*'))

query = df_streaming_visits.writeStream \
          .outputMode("append") \
          .format("console") \
          .start()

query.awaitTermination()
