from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.landing import google_sheet_to_minio_etl

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
    description='DAG responsável pelo ETL do case breweries',
    schedule=timedelta(days=1),  # Usando "schedule" no lugar de "schedule_interval"
    catchup=False
)
def main_dag():
    google_sheets = ["cars_24_combined"]  # Altere para o nome das abas da sua planilha
    bucket_name = 'landing'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '10v24Oj9fgciYq8CB-9Bv--bkOuEA41xP8tbQD-DId8w'

    with TaskGroup("group_task_sheets", tooltip="Tasks processadas do Google Sheets para MinIO, salvando em .parquet") as group_task_sheets:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'task_sheets_{sheets_name.replace(" ", "_").replace(",", "").replace("(", "").replace(")", "").replace("&", "and")}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheets_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    group_task_sheets  # Definir a ordem de execução, se necessário

main_dag_instance = main_dag()