# transformation/postgres_loader.py

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
R2_ACCESS_KEY   = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY   = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT     = os.getenv("R2_ENDPOINT")
R2_BUCKET       = os.getenv("R2_BUCKET")

PG_HOST         = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT         = os.getenv("POSTGRES_PORT", "5432")
PG_DB           = os.getenv("POSTGRES_DB", "newsdb")
PG_USER         = os.getenv("POSTGRES_USER", "newsuser")
PG_PASSWORD     = os.getenv("POSTGRES_PASSWORD", "newspassword")

JDBC_URL        = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

JAR_PATH = ",".join([
    "/home/kirthic/news-pipeline/infra/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/kafka-clients-3.4.0.jar",
    "/home/kirthic/news-pipeline/infra/jars/hadoop-aws-3.3.4.jar",
    "/home/kirthic/news-pipeline/infra/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/home/kirthic/news-pipeline/infra/jars/commons-pool2-2.11.1.jar",
    "/home/kirthic/news-pipeline/infra/jars/postgresql-42.7.3.jar",
])

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("GoldToPostgres")
    .config("spark.jars", JAR_PATH)
    .config("spark.hadoop.fs.s3a.access.key", R2_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", R2_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", R2_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Read Gold Parquet from R2 ─────────────────────────────────────────────────
gold_path = f"s3a://{R2_BUCKET}/news-pipeline/gold/"
print(f"Reading gold layer from: {gold_path}")

df = spark.read.parquet(gold_path)
df.printSchema()
print(f"Total gold records: {df.count()}")

# ── Write to PostgreSQL ───────────────────────────────────────────────────────
JDBC_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

def write_table(dataframe, table_name, mode="overwrite"):
    print(f"Writing {dataframe.count()} rows → {table_name} [{mode}]")
    (
        dataframe.write
        .jdbc(url=JDBC_URL, table=table_name, mode=mode, properties=JDBC_PROPS)
    )
    print(f"  ✓ {table_name} done")

# Write the full gold table
write_table(df, "raw_gold_articles")

# ── Derived tables (pre-aggregated for dbt) ───────────────────────────────────
from pyspark.sql.functions import to_date, avg, count

# Daily sentiment aggregates
daily = (
    df.withColumn("publish_date", to_date(col("published_at")))
    .groupBy("publish_date", "category")
    .agg(
        count("*").alias("article_count"),
        avg("sentiment_score").alias("avg_sentiment_score"),
    )
)
write_table(daily, "stg_daily_sentiment")

print("\n✅ All tables loaded into PostgreSQL successfully.")
spark.stop()