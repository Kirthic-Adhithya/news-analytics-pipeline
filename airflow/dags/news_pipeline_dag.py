from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/home/kirthic/news-pipeline')

default_args = {
    "owner": "kirthic",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_cleaning():
    from transformation.pyspark_cleaner import run_cleaning_job
    run_cleaning_job()

def run_sentiment():
    from transformation.sentiment_enricher import run_sentiment_job
    run_sentiment_job()

def run_neon_sync():
    from transformation.neon_sync import main
    main()

with DAG(
    dag_id="news_pipeline",
    default_args=default_args,
    description="Daily news pipeline: clean + sentiment + neon sync",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["news", "etl"],
) as dag:

    clean_task = PythonOperator(
        task_id="clean_bronze_to_silver",
        python_callable=run_cleaning,
    )

    sentiment_task = PythonOperator(
        task_id="enrich_with_sentiment",
        python_callable=run_sentiment,
    )

    neon_sync_task = PythonOperator(
        task_id="sync_to_neon",
        python_callable=run_neon_sync,
    )

    clean_task >> sentiment_task >> neon_sync_task
