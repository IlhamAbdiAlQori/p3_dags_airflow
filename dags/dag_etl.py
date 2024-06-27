from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data():
    # Fungsi untuk mengekstrak data dari PostgreSQL
    pass

def transform_data():
    # Fungsi untuk transformasi data
    pass

with DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG untuk memindahkan data dari PostgreSQL ke BigQuery',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 6, 25),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_to_gcs_task = PostgresToGCSOperator(
        task_id='load_to_gcs',
        postgres_conn_id='DataOps',
        sql="""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name not like 'datamart%%'""",
        bucket='your_gcs_bucket',
        filename='data/{{ ds }}.json',
        export_format='json',
    )

    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='your_gcs_bucket',
        source_objects=['data/{{ ds }}.json'],
        destination_project_dataset_table='your_project.your_dataset.your_table',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    extract_task >> transform_task >> load_to_gcs_task >> load_to_bq_task

