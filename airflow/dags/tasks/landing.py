import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Connection
from airflow import settings
import os
import time
from airflow.models import Connection
from airflow.settings import Session

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_mariadb_connection():
    session = Session()
    
    # Verifica se a conexão já existe
    existing_conn = session.query(Connection).filter_by(conn_id="mariadb_default").first()
    if existing_conn:
        print("A conexão 'mariadb_default' já existe. Pulando a criação.")
        session.close()
        return

    # Se não existir, cria a conexão
    new_conn = Connection(
        conn_id="mariadb_default",
        conn_type="mysql",
        host="localhost",
        schema="lakeestudo",
        login="mariadb",
        password="maria123",
        port=3306,
        extra='{"charset": "utf8", "ssl": {}}'
    )
    
    session.add(new_conn)
    session.commit()
    session.close()
    print("Conexão 'mariadb_default' criada com sucesso.")

def wait_for_mariadb():
    retries = 5
    for i in range(retries):
        try:
            mariadb_hook = MySqlHook(mysql_conn_id='mariadb_default')
            conn = mariadb_hook.get_conn()
            conn.ping(reconnect=True)  # Garante a conexão ativa
            logger.info("Conectado ao MariaDB com sucesso.")
            return
        except Exception as e:
            logger.warning(f"Tentativa {i + 1} de conexão falhou: {e}")
            time.sleep(5)
    raise ConnectionError("Não foi possível conectar ao MariaDB após várias tentativas.")

def google_sheet_to_minio_etl(sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key):
    create_mariadb_connection()
    wait_for_mariadb()
    mariadb_hook = MySqlHook(mysql_conn_id='mariadb_default')

    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    def get_google_sheet_data(sheet_id, sheet_name):
        try:
            scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds = Credentials.from_service_account_file('/opt/airflow/config_airflow/googleAPIkey.json', scopes=scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            data = sheet.get_all_records()
            if not data:
                raise ValueError(f"Nenhum dado foi retornado para a planilha {sheet_name}")
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            logger.error(f"Erro ao obter dados da planilha do Google: {e}")
            raise

    try:
        df = get_google_sheet_data(sheet_id, sheet_name)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=bucket_name, Key=f"{sheet_name}/data.parquet", Body=parquet_buffer.getvalue())
        insert_data_into_mariadb(df, sheet_name)
    except Exception as e:
        logger.error(f"Erro ao processar a planilha {sheet_name}: {e}")
        raise

def create_table_mariadb(table_name):
    mariadb_hook = MySqlHook(mysql_conn_id='mariadb_default')
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        car_name VARCHAR(255),
        year INT,
        distance VARCHAR(255),
        owner VARCHAR(255),
        fuel VARCHAR(255),
        location VARCHAR(255),
        drive VARCHAR(255),
        type VARCHAR(255),
        price DECIMAL(10,2)
    );
    '''
    mariadb_hook.run(create_table_query)
    logger.info(f"Tabela {table_name} criada com sucesso.")

def insert_data_into_mariadb(df, table_name):
    create_table_mariadb(table_name)
    mariadb_hook = MySqlHook(mysql_conn_id='mariadb_default')
    try:
        df = df.where(pd.notnull(df), None)  # Converter NaN para None
        insert_query = f'''
        INSERT INTO {table_name} (car_name, year, distance, owner, fuel, location, drive, type, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        data_tuples = [
            (
                row['car_name'], row['year'], row['distance'], row['owner'],
                row['fuel'], row['location'], row['drive'], row['type'], row['price']
            )
            for _, row in df.iterrows()
        ]
        mariadb_hook.insert_rows(table=table_name, rows=data_tuples, target_fields=[
            'car_name', 'year', 'distance', 'owner', 'fuel', 'location', 'drive', 'type', 'price'
        ])
        logger.info(f"Dados inseridos na tabela {table_name} com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {table_name}: {e}")
        raise
