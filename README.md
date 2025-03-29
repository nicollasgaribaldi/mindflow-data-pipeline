# MindFlow - Data Pipeline for Mental Health Analysis

## 📊 Pipeline Architecture
Below is the graphical representation of this project's architecture:

**Architecture Diagram**

In this architecture, data is extracted from a Google Sheets spreadsheet, transformed to add the `user_id` column, and segmented into Bronze, Silver, and Gold layers. Then, the data is loaded into an S3 bucket, and visualization is configured via Metabase.

## 📂 Project Structure
The project structure is organized as follows:

```
/MINDFLOW-DATA-PIPELINE
│
├── airflow/
│   ├── config_airflow/
│   │   └── airflow.Dockerfile           # Dockerfile customizado para o Airflow
│   ├── dags/
│   │   ├── tasks/
│   │   |   └── __init__.py
│   │   |   └── landing.py
│   │   |   └── processing.py        
│   │   └── dag_main.py                  # Arquivo principal da DAG contendo as extrações e as transformações
├── dataset/                            # Nessa pasta existe os arquivos que utilizei para exploração dos dados e tratamentos realizados
│   |   ├── Analysis/
│   |   |   ├── landing/
│   |   |   ├── bronze/
│   |   |   ├── silver/
│   |   |   ├── gold/
│   |   └── Mental_Health_Lifestyle_Dataset.csv   # Raw Dataset
├── image/                            
├── venv/
├── docker-compose.yaml                  # Estrutura e requisitos iniciais em container do projeto.
├── requirements.txt                     # Responsavel pelas lib's principais para a criação do projeto.
├── README.md                            # Documentação do projeto, utilizada para o entendimento e funcionamento do mesmo.
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

## ![s3](https://github.com/user-attachments/assets/5a767046-d971-4b7d-97f2-02bb641f4b30) Minio

![MinIO](https://github.com/user-attachments/assets/3343c493-87e0-463d-92a4-ee8a3802cc86)

## ![Postgres](https://github.com/user-attachments/assets/79b963dc-9ced-4aa1-b2bd-880588012a6f) Postgres

## ![metabase](https://github.com/user-attachments/assets/aab28b91-2e03-408b-ac4c-54b40d4056ba) Metabase




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
