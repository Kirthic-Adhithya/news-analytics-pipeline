from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *
from dotenv import load_dotenv
import os

load_dotenv()

R2_ACCESS_KEY   = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY   = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET       = os.getenv("R2_BUCKET")
R2_ENDPOINT     = os.getenv("R2_ENDPOINT")
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "raw_news"
S3_BRONZE_PATH  = f"s3a://{R2_BUCKET}/news-pipeline/bronze/"
CHECKPOINT_PATH = f"s3a://{R2_BUCKET}/news-pipeline/checkpoints/bronze/"

JAR_PATH = ",".join([
    "/home/kirthic/news-pipeline/infra/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/kafka-clients-3.4.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/hadoop-aws-3.3.4.jar",
    "/home/kirthic/news-pipeline/infra/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/home/kirthic/news-pipeline/infra/jars/commons-pool2-2.11.1.jar",
])

schema = StructType([
    StructField("article_id",   StringType()),
    StructField("title",        StringType()),
    StructField("description",  StringType()),
    StructField("url",          StringType()),
    StructField("source",       StringType()),
    StructField("category",     StringType()),
    StructField("author",       StringType()),
    StructField("published_at", StringType()),
    StructField("ingested_at",  StringType()),
])

spark = SparkSession.builder \
    .appName("NewsStreamConsumer") \
    .config("spark.jars", JAR_PATH) \
    .config("spark.hadoop.fs.s3a.access.key", R2_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", R2_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", R2_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("load_timestamp", current_timestamp())

query = parsed.writeStream \
    .format("json") \
    .option("path", S3_BRONZE_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .partitionBy("category") \
    .trigger(processingTime="2 minutes") \
    .start()

print("Spark Streaming consumer started. Writing to R2 bronze layer...")
print(f"Output path: {S3_BRONZE_PATH}")
query.awaitTermination()