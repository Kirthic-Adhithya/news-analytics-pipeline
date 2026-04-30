from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_date
from pyspark.sql.types import StringType, FloatType, StructType, StructField
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

def run_sentiment_job():
    spark = SparkSession.builder \
        .appName("SentimentEnricher") \
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

    silver_path = f"s3a://{R2_BUCKET}/news-pipeline/silver/"
    gold_path   = f"s3a://{R2_BUCKET}/news-pipeline/gold/"

    print(f"Reading silver layer from: {silver_path}")
    df = spark.read.parquet(silver_path)
    print(f"Records to enrich: {df.count()}")

    # Load model once on driver then broadcast via UDF
    from transformers import pipeline as hf_pipeline
    sentiment_pipe = hf_pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        truncation=True,
        max_length=512
    )

    sentiment_schema = StructType([
        StructField("label", StringType()),
        StructField("score", FloatType()),
    ])

    def get_sentiment(text):
        if not text or len(text.strip()) < 5:
            return ("NEUTRAL", 0.5)
        try:
            result = sentiment_pipe(text[:512])[0]
            return (result["label"], float(result["score"]))
        except:
            return ("NEUTRAL", 0.5)

    sentiment_udf = udf(get_sentiment, sentiment_schema)

    enriched = df \
        .withColumn("sentiment", sentiment_udf(col("description"))) \
        .withColumn("sentiment_label", col("sentiment.label")) \
        .withColumn("sentiment_score", col("sentiment.score")) \
        .drop("sentiment")

    print(f"Writing gold layer to: {gold_path}")
    enriched.write \
        .mode("overwrite") \
        .partitionBy("category") \
        .parquet(gold_path)

    print(f"Gold layer written! Sample sentiment distribution:")
    enriched.groupBy("sentiment_label").count().show()
    spark.stop()

if __name__ == "__main__":
    run_sentiment_job()