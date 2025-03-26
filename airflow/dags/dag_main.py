from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.landing import google_sheet_to_minio_etl

# Configuração padrão da DAG
default_args = {
    'owner': 'Nicollas Garibaldi',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='main_dag',
    default_args=default_args,
    description='DAG responsável pelo ETL do dataset de saúde mental',
    schedule=timedelta(days=1),
    catchup=False
)
def main_dag():
    google_sheets = ["Mental Health Lifestyle"]
    bucket_name = 'landing'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '1cK7S70LhKyMBWyiaKIazsqkaRCgW0KXrrX8bkRJKQ5Y'

    with TaskGroup("group_task_sheets", tooltip="Tasks processadas do Google Sheets para MinIO, salvando em .parquet") as group_task_sheets:
        for sheet_name in google_sheets:
            PythonOperator(
                task_id=f'task_sheets_{sheet_name.replace(" ", "_").replace(",", "").replace("(", "").replace(")", "").replace("&", "and")}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    group_task_sheets

main_dag_instance = main_dag()
