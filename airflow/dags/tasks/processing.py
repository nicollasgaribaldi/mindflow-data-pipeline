import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging
import uuid
from sqlalchemy import create_engine
import pymysql
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_engine():
    """Cria e retorna a conexão com o MariaDB"""
    return create_engine(
        "mysql+pymysql://mariadb:maria123@local_mariadb:3306/lakeestudo?charset=utf8mb4",
        pool_pre_ping=True,
        pool_recycle=3600
    )

def save_to_mariadb(df, table_name):
    """Salva um DataFrame no MariaDB com tratamento de tipos"""
    try:
        engine = get_db_engine()
        
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype(str)
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=1000,
            method='multi'
        )
        logger.info(f"Dados salvos com sucesso na tabela {table_name}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar no MariaDB: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

def google_sheet_to_minio_and_mariadb_etl(sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key):
    """Extrai dados do Google Sheets e salva no MinIO e MariaDB"""
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    def get_google_sheet_data(sheet_id, sheet_name):
        try:
            scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds = Credentials.from_service_account_file(
                '/opt/airflow/config_airflow/googleAPIkey.json', 
                scopes=scope
            )
            client = gspread.authorize(creds)
            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            data = sheet.get_all_records()
            
            if not data:
                raise ValueError(f"Nenhum dado foi retornado para a planilha {sheet_name}")
                
            df = pd.DataFrame(data)

            if 'user_id' not in df.columns:
                logger.warning("'user_id' não encontrado. Criando identificadores únicos.")
                df.insert(0, 'user_id', [str(uuid.uuid4()) for _ in range(len(df))])
            
            return df
        except Exception as e:
            logger.error(f"Erro ao obter dados da planilha do Google: {e}")
            raise
    
    try:
        df = get_google_sheet_data(sheet_id, sheet_name)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(
            Bucket=bucket_name,
            Key=f"landing/{sheet_name}/lnd_{sheet_name}.parquet",
            Body=parquet_buffer.getvalue()
        )
        logger.info(f"Arquivo landing salvo no MinIO: lnd_{sheet_name}.parquet")
        
        save_to_mariadb(df, f"landing_{sheet_name.lower()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Erro no processamento da landing layer: {e}")
        raise

def process_bronze_layer(bucket_name, file_key, endpoint_url, access_key, secret_key):
    """Processa a camada bronze a partir dos dados landing"""
    try:
        minio_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        obj = minio_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        df['ingestion_timestamp'] = datetime.now()
        logger.info(f"Colunas após transformação: {df.columns.tolist()}")
        
        bronze_buffer = io.BytesIO()
        df.to_parquet(bronze_buffer, index=False)
        bronze_buffer.seek(0)
        
        bronze_key = file_key.replace('landing', 'bronze')
        minio_client.put_object(
            Bucket=bucket_name,
            Key=bronze_key,
            Body=bronze_buffer.getvalue()
        )
        logger.info(f"Arquivo bronze salvo no MinIO: {bronze_key}")
        
        table_name = "bronze_" + file_key.split('/')[-1].replace('.parquet', '')
        save_to_mariadb(df, table_name)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro no processamento bronze: {str(e)}")
        raise

def process_silver_layer(df, bucket_name, file_key, endpoint_url, access_key, secret_key):
    """Processa a camada silver a partir dos dados bronze"""
    try:
        minio_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        df = df.copy()
        df.fillna({
            'age': df['age'].median(),
            'happiness_score': df['happiness_score'].mean()
        }, inplace=True)
        
        silver_buffer = io.BytesIO()
        df.to_parquet(silver_buffer, index=False)
        silver_buffer.seek(0)
        
        silver_key = file_key.replace('landing', 'silver')
        minio_client.put_object(
            Bucket=bucket_name,
            Key=silver_key,
            Body=silver_buffer.getvalue()
        )
        logger.info(f"Arquivo silver salvo no MinIO: {silver_key}")
        
        table_name = "silver_" + file_key.split('/')[-1].replace('.parquet', '')
        save_to_mariadb(df, table_name)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro no processamento silver: {str(e)}")
        raise

def process_gold_layer(df, bucket_name, file_key, endpoint_url, access_key, secret_key):
    """Processa a camada gold com modelagem dimensional"""
    try:
        minio_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        if 'user_id' not in df.columns:
            raise ValueError(f"Coluna 'user_id' não encontrada. Colunas disponíveis: {df.columns.tolist()}")
        
        fact_table = df[['user_id', 'age', 'happiness_score']]
        dim_table = df[['user_id', 'country', 'gender']]
        
        fact_buffer = io.BytesIO()
        fact_table.to_parquet(fact_buffer, index=False)
        fact_buffer.seek(0)
        
        fact_key = file_key.replace('landing', 'gold/fact_table')
        minio_client.put_object(
            Bucket=bucket_name,
            Key=fact_key,
            Body=fact_buffer.getvalue()
        )
        logger.info(f"Fact table salva no MinIO: {fact_key}")
        
        dim_buffer = io.BytesIO()
        dim_table.to_parquet(dim_buffer, index=False)
        dim_buffer.seek(0)
        
        dim_key = file_key.replace('landing', 'gold/dim_table')
        minio_client.put_object(
            Bucket=bucket_name,
            Key=dim_key,
            Body=dim_buffer.getvalue()
        )
        logger.info(f"Dimension table salva no MinIO: {dim_key}")
        
        base_name = file_key.split('/')[-1].replace('.parquet', '')
        save_to_mariadb(fact_table, f"gold_fact_{base_name}")
        save_to_mariadb(dim_table, f"gold_dim_{base_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Erro no processamento gold: {str(e)}")
        raise