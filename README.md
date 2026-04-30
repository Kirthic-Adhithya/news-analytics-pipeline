# Real-Time News Analytics Pipeline

> An end-to-end data engineering pipeline that ingests live news articles, streams them through Apache Kafka, processes with PySpark, stores on cloud object storage, and serves analytics via a BI dashboard.

![Status](https://img.shields.io/badge/Status-In%20Progress-yellow)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)
![Kafka](https://img.shields.io/badge/Kafka-7.4.0-black)

---

## Architecture

```
NewsAPI → Kafka Producer → Apache Kafka → Spark Structured Streaming
                                                      ↓
                                          Cloudflare R2 (Bronze Layer)
                                                      ↓
                                     PySpark Cleaner + Sentiment Enricher
                                                      ↓
                                          Cloudflare R2 (Silver/Gold Layer)
                                                      ↓
                                        Apache Airflow (Orchestration)
                                                      ↓
                                              dbt (Data Models)
                                                      ↓
                                           Metabase Dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | NewsAPI, Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Cloud Storage | Cloudflare R2 (S3-compatible) |
| Orchestration | Apache Airflow |
| Transformation | PySpark, dbt |
| Sentiment Analysis | HuggingFace Transformers (DistilBERT) |
| Dashboard | Metabase |
| Infrastructure | Docker, WSL2 |

---

## Completed Phases

### Kafka Ingestion Layer
- Set up Apache Kafka cluster using Docker Compose (Zookeeper + Kafka + Kafka UI)
- Built a Kafka producer that polls 5 news categories from NewsAPI every 30 minutes
- Streams structured JSON records into `raw_news` Kafka topic with 3 partitions
- Verified real-time message flow with 90+ articles per poll cycle

### Spark Streaming → Cloud Storage
- Configured PySpark 3.5.0 with Structured Streaming to consume from Kafka
- Integrated S3A connector to write to Cloudflare R2 (S3-compatible object storage)
- Bronze layer data partitioned by category (technology, business, science, health, entertainment)
- 30+ JSON files landing in R2 per trigger with proper partition structure

---

## In Progress

### Batch Transformation
- Apache Airflow DAG for daily orchestration
- PySpark cleaning job (deduplication, null handling, timestamp normalization)
- Sentiment enrichment using HuggingFace DistilBERT model

### Warehouse + dbt Models
- dbt models: `fact_articles`, `dim_sources`, `agg_daily_trends`
- dbt tests for data quality validation

### Dashboard
- Metabase dashboards: trending topics, sentiment over time, source comparisons

---

## Project Structure

```
news-analytics-pipeline/
├── ingestion/
│   ├── kafka_producer.py          # NewsAPI → Kafka
│   └── spark_streaming_consumer.py # Kafka → R2 Bronze
├── transformation/
│   ├── pyspark_cleaner.py         # Bronze → Silver
│   ├── sentiment_enricher.py      # Silver → Gold
│   └── redshift_loader.py         # Gold → Warehouse
├── airflow/dags/
│   └── news_pipeline_dag.py       # Orchestration DAG
├── dbt/models/
│   ├── staging/
│   ├── marts/
│   └── aggregates/
├── docker-compose.yml             # Kafka cluster setup
└── README.md
```

---

## Getting Started

### Prerequisites
- WSL2 (Ubuntu 24.04)
- Docker Desktop with WSL2 integration
- Python 3.12+
- Java 17

### Setup

```bash
# Clone the repo
git clone https://github.com/Kirthic-Adhithya/news-analytics-pipeline.git
cd news-analytics-pipeline

# Install dependencies
uv venv && source .venv/bin/activate
uv add kafka-python requests python-dotenv pyspark boto3

# Download required JARs
mkdir -p infra/jars && cd infra/jars
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
```

### Environment Variables

Create a `.env` file:
```env
NEWSAPI_KEY=your_key
KAFKA_BOOTSTRAP=localhost:9092
R2_ACCESS_KEY_ID=your_key
R2_SECRET_ACCESS_KEY=your_secret
R2_BUCKET=your_bucket
R2_ENDPOINT=https://your_account_id.r2.cloudflarestorage.com
```

### Run

```bash
# Start Kafka
docker-compose up -d

# Terminal 1 — Start producer
python3 ingestion/kafka_producer.py

# Terminal 2 — Start Spark consumer
python3 ingestion/spark_streaming_consumer.py
```

---

## Key Features

- **Real-time streaming** — Articles flow from NewsAPI into Kafka within seconds
- **Scalable storage** — S3-compatible partitioned storage with bronze/silver/gold layers
- **Sentiment analysis** — NLP enrichment using DistilBERT on every article
- **Production patterns** — Checkpointing, partitioning, data quality tests
- **Full orchestration** — Airflow DAG managing the complete pipeline lifecycle

---

## What I Learned

- Designing and implementing a Lambda architecture with both streaming and batch layers
- Managing JAR dependencies and version compatibility in PySpark
- Working with S3-compatible object storage APIs
- Structuring a data lake with medallion architecture (bronze/silver/gold)
- Orchestrating multi-step data pipelines with Apache Airflow

---

## Author

**Kirthic Adhithya**  
[GitHub](https://github.com/Kirthic-Adhithya)