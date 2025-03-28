from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import os
import logging
import sys

# Adiciona o caminho para a pasta tasks ao PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

# Importe as funções com tratamento de erro
try:
    from tasks.landing import google_sheet_to_minio_and_mariadb_etl
    from tasks.processing import (
        process_bronze_layer,
        process_silver_layer,
        process_gold_layer
    )
except ImportError as e:
    logger.error(f"Erro ao importar funções: {e}")
    raise

default_args = {
    'owner': 'Nicollas Garibaldi',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_minio_config():
    """Obtém configurações do MinIO com fallback robusto"""
    try:
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minio@1234!")
        return endpoint, access_key, secret_key
    except Exception as e:
        logger.error(f"Erro ao obter configurações: {e}")
        raise

@dag(
    dag_id='mental_health_data_pipeline',
    default_args=default_args,
    description='Pipeline completo ETL para dados de saúde mental',
    schedule_interval="0 11 * * *",
    catchup=False,
    tags=['mental_health', 'etl']
)
def mental_health_pipeline():
    endpoint_url, access_key, secret_key = get_minio_config()
    google_sheets = ['Mental_Health_Lifestyle_Dataset']
    bucket_name = 'landing'
    sheet_id = '1cK7S70LhKyMBWyiaKIazsqkaRCgW0KXrrX8bkRJKQ5Y'

    # Task Group para extração
    with TaskGroup("landing_group") as landing_group:
        for sheet_name in google_sheets:
            PythonOperator(
                task_id=f'landing_{sheet_name.lower()}',
                python_callable=google_sheet_to_minio_and_mariadb_etl,
                op_args=[sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    # Task Group para processamento
    with TaskGroup("processing_group") as processing_group:
        for sheet_name in google_sheets:
            file_key = f"landing/{sheet_name}/lnd_{sheet_name}.parquet"
            
            bronze_task = PythonOperator(
                task_id=f'bronze_{sheet_name.lower()}',
                python_callable=process_bronze_layer,
                op_args=[bucket_name, file_key, endpoint_url, access_key, secret_key]
            )
            
            # No seu dag_main.py, atualize a definição da silver_task:
            silver_task = PythonOperator(
                task_id=f'silver_{sheet_name.lower()}',
                python_callable=process_silver_layer,
                op_kwargs={
                    'df': bronze_task.output,  # DataFrame da camada bronze
                    'bucket_name': bucket_name,
                    'file_key': file_key,
                    'endpoint_url': endpoint_url,
                    'access_key': access_key,
                    'secret_key': secret_key  # Adicionando o parâmetro faltante
                }
            )
            
            gold_task = PythonOperator(
                task_id=f'gold_{sheet_name.lower()}',
                python_callable=process_gold_layer,
                op_kwargs={
                    'df': silver_task.output,
                    'bucket_name': bucket_name,
                    'file_key': file_key,
                    'endpoint_url': endpoint_url,
                    'access_key': access_key,
                    'secret_key': secret_key
                }
            )
            
            bronze_task >> silver_task >> gold_task

    landing_group >> processing_group

pipeline = mental_health_pipeline()