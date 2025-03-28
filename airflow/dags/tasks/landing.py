import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging
import uuid
from sqlalchemy import create_engine
import pymysql

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def google_sheet_to_minio_and_mariadb_etl(sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key):
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

            # Criar a coluna 'user_id' se não existir
            if 'user_id' not in df.columns:
                logger.warning("'user_id' não encontrado. Criando identificadores únicos.")
                df.insert(0, 'user_id', [str(uuid.uuid4()) for _ in range(len(df))])
            
            return df
        except Exception as e:
            logger.error(f"Erro ao obter dados da planilha do Google: {e}")
            raise
    
    try:
        df = get_google_sheet_data(sheet_id, sheet_name)
        
        # Salvar no MinIO
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(
            Bucket=bucket_name,
            Key=f"landing/{sheet_name}/lnd_{sheet_name}.parquet",
            Body=parquet_buffer.getvalue()
        )
        logger.info(f"Arquivo lnd_{sheet_name}.parquet salvo no bucket {bucket_name}")
        
        # Salvar no MariaDB
        save_to_mariadb(df, f"landing_{sheet_name.lower()}")
        
    except Exception as e:
        logger.error(f"Erro ao processar a planilha lnd_{sheet_name}: {e}")
        raise

def save_to_mariadb(df, table_name):
    try:
        engine = create_engine(
            "mysql+pymysql://mariadb:maria123@local_mariadb:3306/lakeestudo?charset=utf8mb4",
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        df = df.apply(lambda x: x.astype(str) if x.dtype == 'object' else x)
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=1000,
            method='multi'
        )
        logger.info(f"Dados salvos com sucesso na tabela {table_name}")
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Erro ao salvar no MariaDB: {str(e)}")
        if 'engine' in locals():
            engine.dispose()
        raise
