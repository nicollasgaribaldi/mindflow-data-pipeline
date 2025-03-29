# MindFlow - Data Pipeline for Mental Health Analysis

## 📊 Pipeline Architecture
Below is the graphical representation of this project's architecture:

**Architecture Diagram**

![Architecture Diagram](https://github.com/user-attachments/assets/2334c84b-a353-4454-b4a4-9c0e98836dfa)

In this architecture, data is extracted from a Google Sheets spreadsheet, transformed to add the `user_id` column, and segmented into Bronze, Silver, and Gold layers. Then, the data is loaded into an S3 bucket, and visualization is configured via Metabase.

## 📂 Project Structure
The project structure is organized as follows:

```
/MINDFLOW-DATA-PIPELINE  
│  
├── airflow/                             # Contains all Airflow-related configurations and DAGs  
│   ├── config_airflow/                  # Airflow configuration files and custom setups  
│   │   └── airflow.Dockerfile           # Custom Dockerfile for Airflow (extends the official image with additional dependencies)  
│   ├── dags/                            # Directory for Airflow DAGs (Directed Acyclic Graphs)  
│   │   ├── tasks/                       # Modular task definitions (reusable across DAGs)  
│   │   |   └── __init__.py             # Python package marker  
│   │   |   └── landing.py              # Tasks related to raw data ingestion (landing zone operations)  
│   │   |   └── processing.py           # Tasks for data processing (cleaning, transformations, etc.)  
│   │   └── dag_main.py                 # Main DAG orchestrating the data pipeline (extract → transform → load)  
│  
├── dataset/                            # Contains raw data, processed datasets, and analysis artifacts  
│   |   ├── Analysis/                   # Processed data layers (medallion architecture: landing → bronze → silver → gold)  
│   |   |   ├── landing/                # Raw data as ingested (immutable)  
│   |   |   ├── bronze/                 # Initial cleaned/validated data (schema enforcement, basic fixes)  
│   |   |   ├── silver/                 # Enriched/transformed data (business logic applied)  
│   |   |   ├── gold/                   # Final aggregated/analytical datasets (ready for consumption)  
│   |   └── Mental_Health_Lifestyle_Dataset.csv   # Original raw dataset (source file)  
│  
├── image/                              # Stores visual assets (e.g., diagrams, charts) for documentation  
├── venv/                               # Python virtual environment (dependency isolation)  
│  
├── docker-compose.yaml                 # Defines Airflow services (scheduler, webserver, etc.) and dependencies  
├── requirements.txt                    # Python dependencies for the project (e.g., pandas, Airflow, custom libs)  
├── README.md                           # Project overview, setup instructions, and pipeline documentation  
```

## 🛠️ Technologies Used
- **Google Sheets**: Data source for the pipeline.
- **Python**: Main language for data extraction, transformation, and loading.
- **AWS S3**: Structured data storage in Bronze, Silver, and Gold layers.
- **Metabase**: BI tool for visualization and analysis of processed data.
- **Docker**: For containerization and environment reproducibility.

## 🐳 Docker
The project is configured to run in a Docker environment. The `docker-compose.yaml` file defines the necessary services for running the pipeline and Metabase.

![Docker](https://github.com/user-attachments/assets/759bbcfa-e349-4687-ac15-b12ea1a53488)

## ![airflow2](https://github.com/user-attachments/assets/159a8038-6bf5-43fd-b0b1-328a6896b0f6) Airflow

- **DAG:** The Directed Acyclic Graph (DAG) is defined in the `airflow/dags/` folder. The main file, `mindflow_dag.py`, orchestrates the data pipeline tasks, including data extraction, transformation, and loading.  
- **Tasks:** The pipeline tasks are modularized within the `airflow/tasks/` directory. For example, `task_data_transformation.py` contains the logic for processing and structuring the dataset into Bronze, Silver, and Gold layers.  
- **Configurations:** All Airflow-specific settings and customizations are located in the `airflow/config/` directory, ensuring proper environment setup and execution.  

![Airflow](https://github.com/user-attachments/assets/0c5b4195-2f2d-4f79-8de8-060a2cd343a7)

## ![s3](https://github.com/user-attachments/assets/5a767046-d971-4b7d-97f2-02bb641f4b30) Minio

- **Storage:** Data is stored and managed across the **Bronze, Silver, and Gold** buckets, following the project’s scope and requirements. Each layer ensures a structured and incremental data refinement process.  
- **Medallion Architecture:** A data design pattern used in the **data lake**, aimed at progressively improving the structure and quality of the data through **Bronze → Silver → Gold** layers.  
- **Configurations:** All **Metabase** configurations and customizations are defined in the `docker-compose.yml` file, ensuring seamless deployment and integration.  

![MinIO](https://github.com/user-attachments/assets/3343c493-87e0-463d-92a4-ee8a3802cc86)

## ![Postgres](https://github.com/user-attachments/assets/79b963dc-9ced-4aa1-b2bd-880588012a6f) Postgres

- **Data-Viz:** Creation and availability of data visualizations, with connections to PostgreSQL, designed to meet the needs of different consumer groups.  
- **Users:** Configuration of access control to layers and tables based on user groups, ensuring proper data access management.  
- **Configurations:** All Metabase-specific configurations and customizations are defined in the `docker-compose.yml` file.  

![DB](https://github.com/user-attachments/assets/b897b181-a202-48f3-a9d0-a05fd1c8bad1)

## ![metabase](https://github.com/user-attachments/assets/aab28b91-2e03-408b-ac4c-54b40d4056ba) Metabase

- **Data-Viz:** Creation and availability of data visualizations, catering to various types of consumers by providing easy access to insights.  
- **Configurations:** All Metabase-specific configurations and customizations are defined in the `docker-compose.yml` file.  

![Mindflow-Dashboard-1](https://github.com/user-attachments/assets/ffb2ee0d-0294-4630-a5ca-d5e71ecfc6bd)

## 🚀 How to Get Started
1. Clone the repository:
   ```bash
   git clone https://github.com/nicollasgaribaldi/mindflow-data-pipeline.git
   ```
2. Navigate to the project directory:
   ```bash
   cd mindflow-data-pipeline
   ```
3. Start the containers with Docker:
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

7. To stop the Docker containers:
   ```bash
   docker-compose down -v
   ```

## 📚 Documentation
- [Official Docker Documentation](https://docs.docker.com/)
- [Official Metabase Documentation](https://www.metabase.com/docs/)
- [Official AWS S3 Documentation](https://docs.aws.amazon.com/s3/index.html)

## **📜 Dataset**  
Kaggle: 🧠 [Mental Health and Lifestyle Habits (2019-2024)](https://www.kaggle.com/datasets/mental-health-and-lifestyle)
