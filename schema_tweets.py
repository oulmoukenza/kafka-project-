from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


#commande consumer sbark master  : root@37f6bbc4a323:/usr/bin/spark-3.0.0-bin-hadoop3.2/bin# ./spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /opt/workspace/tweetconsumer.py

spark = SparkSession.builder\
.appName('window_5min')\
.getOrCreate()

# Reduce logging verbosity
spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "topictweet"
SCHEMA = StructType([
    StructField("content", StringType())])

df_velib_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("json")) \
    .select("json.*")

df_velib_stream.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()