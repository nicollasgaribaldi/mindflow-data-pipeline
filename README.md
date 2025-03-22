# MindFlow - Data Pipeline for Mental Health Analysis  

## 📌 About the Project  
**MindFlow** is a data pipeline developed to analyze patterns and correlations between mental health and lifestyle. The project processes a dataset containing information on daily habits, emotional well-being, and factors affecting mental health, providing valuable insights through interactive dashboards.  

## 🚀 Technologies Used  
- **Apache Airflow** - Workflow orchestration  
- **MinIO** - Object storage (S3-compatible)  
- **MariaDB** - Relational database for structured storage  
- **Metabase** - Interactive dashboards and data visualization  
- **Pandas/PySpark** - Data processing and transformation  
- **Docker & Docker Compose** - Isolated execution environment  

## 🔧 Pipeline Architecture  
The pipeline follows an **ETL (Extract, Transform, Load)** flow structured as follows:  

1. **Extraction**  
   - The **Mental_Health_Lifestyle_Dataset.csv** file is uploaded to MinIO.  

2. **Transformation**  
   - Data is processed using **Pandas/PySpark** for cleaning, normalization, and enrichment.  

3. **Load**  
   - Transformed data is inserted into **MariaDB** for queries and analysis.  

4. **Visualization**  
   - **Metabase** is used to create interactive dashboards and explore mental health patterns.  

### 📊 Pipeline Flow  
```mermaid
graph TD;
    A[CSV Dataset] -->|Extract| B[MinIO S3 Storage];
    B -->|Transform| C[Data Processing (Pandas/PySpark)];
    C -->|Load| D[MariaDB Database];
    D -->|Visualize| E[Metabase Dashboard];
```

## 🛠️ How to Run the Project  

### 📂 Prerequisites  
Before starting, you need to have:  
- **Docker** and **Docker Compose** installed  
- **Python 3.x**  
- **Pandas/PySpark** for data processing  

### ▶️ Step-by-Step Guide  

1. Clone the repository:  
   ```bash
   git clone https://github.com/your-username/mindflow.git
   cd mindflow
   ```

2. Configure environment variables:  
   Create a `.env` file with the required credentials (see `.env.example` for reference).  

3. Start the services with Docker:  
   ```bash
   docker-compose up -d
   ```

4. Access Airflow for monitoring:  
   - URL: [http://localhost:8080](http://localhost:8080)  
   - Username: `airflow`  
   - Password: `airflow`  

5. Run the DAG in Airflow to start the pipeline.  

6. Access Metabase for data visualization:  
   - URL: [http://localhost:3000](http://localhost:3000)  

## 📜 Dataset  
Kaggle: [🧠 Mental Health and Lifestyle Habits (2019-2024)](https://www.kaggle.com/datasets/atharvasoundankar/mental-health-and-lifestyle-habits-2019-2024) 
