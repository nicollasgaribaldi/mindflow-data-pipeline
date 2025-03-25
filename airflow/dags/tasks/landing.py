import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Connection
from airflow import settings
import time
from airflow.settings import Session

def create_mariadb_connection():
    session = Session()
    existing_conn = session.query(Connection).filter_by(conn_id="mariadb_default").first()
    if existing_conn:
        print("A conexão 'mariadb_default' já existe.")
        session.close()
        return

    new_conn = Connection(
        conn_id="mariadb_default",
        conn_type="mysql",
        host="localhost",
        schema="lakeestudo",
        login="mariadb",
        password="maria123",
        port=3307,
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
            conn.ping(reconnect=True)
            return
        except Exception as e:
            time.sleep(5)
    raise ConnectionError("Não foi possível conectar ao MariaDB.")

def process_google_sheet_data(df):
    df['Age'] = df['Age'].astype(int)
    df['Work Hours per Week'] = df['Work Hours per Week'].astype(int)
    df['Screen Time per Day (Hours)'] = pd.to_numeric(df['Screen Time per Day (Hours)'], errors='coerce')
    df['Social Interaction Score'] = pd.to_numeric(df['Social Interaction Score'], errors='coerce')
    df['Happiness Score'] = pd.to_numeric(df['Happiness Score'], errors='coerce')
    return df

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
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file('/opt/airflow/config_airflow/googleAPIkey.json', scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        return process_google_sheet_data(df)

    df = get_google_sheet_data(sheet_id, sheet_name)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    minio_client.put_object(Bucket=bucket_name, Key=f"{sheet_name}/data.parquet", Body=parquet_buffer.getvalue())
    insert_data_into_mariadb(df, sheet_name)

def create_table_mariadb(table_name):
    mariadb_hook = MySqlHook(mysql_conn_id='mariadb_default')
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        Country VARCHAR(255),
        Age INT,
        Gender VARCHAR(50),
        Exercise_Level VARCHAR(100),
        Diet_Type VARCHAR(100),
        Sleep_Hours VARCHAR(50),
        Stress_Level VARCHAR(50),
        Mental_Health_Condition VARCHAR(100),
        Work_Hours INT,
        Screen_Time FLOAT,
        Social_Interaction FLOAT,
        Happiness_Score FLOAT
    );
    '''
    mariadb_hook.run(create_table_query)

def insert_data_into_mariadb(df, table_name):
    create_table_mariadb(table_name)
    mariadb_hook = MySqlHook(mysql_conn_id='mariadb_default')
    insert_query = f'''
    INSERT INTO {table_name} (Country, Age, Gender, Exercise_Level, Diet_Type, Sleep_Hours,
                              Stress_Level, Mental_Health_Condition, Work_Hours,
                              Screen_Time, Social_Interaction, Happiness_Score)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data_tuples = df.to_records(index=False).tolist()
    mariadb_hook.insert_rows(table=table_name, rows=data_tuples)