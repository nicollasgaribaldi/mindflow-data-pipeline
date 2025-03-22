FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt .
RUN pip install -r requirements.txt

# localhost:8080 - AirFlow (Orquestadror)
# localhost:9000 - Minio (Storage)
# localhost:5433/34 - Postgresql
# localhost:3306 - MariaDB
# localhost:3000 - Metabase