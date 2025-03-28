FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install --no-cache-dir \
    pymysql==1.0.2 \
    sqlalchemy==1.4.41 \
    pandas==1.5.3 \
    mysql-connector-python==8.0.28

# localhost:8080 - AirFlow (Orquestadror)
# localhost:9000 - Minio (Storage)
# localhost:5433/34 - Postgresql
# localhost:3306 - MariaDB
# localhost:3000 - Metabase