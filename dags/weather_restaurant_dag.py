"""
Main Airflow DAG - Weather × Restaurant City Dining Score
Pipeline: Ingest → Format → DBT (3 marts) → Index → Dashboard ready

Schedule: Daily at 06:00 UTC
Cities: Paris, Shanghai, London, New York
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import sys
sys.path.insert(0, '/opt/airflow')

from ingestion.ingest_weather import run_ingestion as ingest_weather
from ingestion.ingest_restaurants import run_ingestion as ingest_restaurants
from ingestion.format_to_postgres import run_formatting
from indexing.index_to_elasticsearch import run_indexing

default_args = {
    "owner": "bigdata_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_restaurant_pipeline",
    description="Daily: Weather x Restaurant -> City Dining Score (Paris, Shanghai, London, New York)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["bigdata", "weather", "restaurant", "dbt", "elasticsearch"],
) as dag:

    start = EmptyOperator(task_id="pipeline_start")

    task_ingest_weather = PythonOperator(
        task_id="ingest_weather",
        python_callable=ingest_weather,
    )

    task_ingest_restaurants = PythonOperator(
        task_id="ingest_restaurants",
        python_callable=ingest_restaurants,
    )

    task_format = PythonOperator(
        task_id="format_to_postgres",
        python_callable=run_formatting,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt run \
                --profiles-dir /opt/airflow/dbt_project \
                --project-dir /opt/airflow/dbt_project
        """,
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt test \
                --profiles-dir /opt/airflow/dbt_project \
                --project-dir /opt/airflow/dbt_project
        """,
    )

    task_index = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=run_indexing,
    )

    end = EmptyOperator(task_id="pipeline_end")

    # Pipeline:
    # start → [ingest_weather, ingest_restaurants] → format → dbt_run → dbt_test → index → end
    start >> [task_ingest_weather, task_ingest_restaurants]
    [task_ingest_weather, task_ingest_restaurants] >> task_format
    task_format >> task_dbt_run >> task_dbt_test >> task_index >> end
