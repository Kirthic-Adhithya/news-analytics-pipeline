from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
from dotenv import load_dotenv
import os

load_dotenv()

R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET     = os.getenv("R2_BUCKET")
R2_ENDPOINT   = os.getenv("R2_ENDPOINT")

JAR_PATH = ",".join([
    "/home/kirthic/news-pipeline/infra/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/kafka-clients-3.4.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/hadoop-aws-3.3.4.jar",
    "/home/kirthic/news-pipeline/infra/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/home/kirthic/news-pipeline/infra/jars/commons-pool2-2.11.1.jar",
])

def run_cleaning_job():
    spark = SparkSession.builder \
        .appName("NewsCleaner") \
        .config("spark.jars", JAR_PATH) \
        .config("spark.hadoop.fs.s3a.access.key", R2_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", R2_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", R2_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    bronze_path = f"s3a://{R2_BUCKET}/news-pipeline/bronze/"
    silver_path = f"s3a://{R2_BUCKET}/news-pipeline/silver/"

    print(f"Reading from bronze: {bronze_path}")
    df = spark.read.json(bronze_path)

    print(f"Raw record count: {df.count()}")

    cleaned = df \
        .dropDuplicates(["article_id"]) \
        .filter(col("title").isNotNull() & (length(col("title")) > 10)) \
        .filter(col("url").isNotNull()) \
        .withColumn("published_at",
            to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            .cast(TimestampType())) \
        .withColumn("title",    trim(col("title"))) \
        .withColumn("source",   trim(lower(col("source")))) \
        .withColumn("category", trim(lower(col("category")))) \
        .withColumn("word_count",
            size(split(col("description"), " "))) \
        .withColumn("processing_date", current_date()) \
        .select(
            "article_id", "title", "description", "url",
            "source", "category", "author", "published_at",
            "word_count", "processing_date"
        )

    print(f"Cleaned record count: {cleaned.count()}")

    cleaned.write \
        .mode("overwrite") \
        .partitionBy("category") \
        .parquet(silver_path)

    print(f"Silver layer written to: {silver_path}")
    spark.stop()

if __name__ == "__main__":
    run_cleaning_job()