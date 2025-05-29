# Data Warehouse Automation With Airflow DAG Project

Welcome to the **Data Warehouse Automation With Airflow DAG Project (Data Engineering)** repository! 🚀  
This project showcases a complete data engineering workflow from ingesting raw data into a Data Lake, transforming it via PySpark, orchestrating pipelines with Apache Airflow, to storing structured data in a Data Warehouse for analytics.  

---
## 📖 Project Overview

This project demonstrates a complete local data warehouse automation pipeline, built using modern open-source tools. It covers the full journey of data from ingestion to analytics and is structured to follow best practices in modularity, orchestration, and scalability.

1. **Data Lake Architecture**: Implements a two-tier data lake structure (raw & clean) using MinIO to mimic S3-like object storage.
2. **ETL/ELT Pipeline**: Uses PySpark scripts to perform transformations and loading processes, triggered via Apache Airflow DAGs.
3. **Data Orchestration**: Airflow automates and monitors the entire data pipeline for repeatability and reliability.
4. **Lightweight Data Warehouse**: DuckDB is used as an embedded OLAP database to store transformed datasets optimized for analysis.

🎯 This repository is ideal for those who want to showcase hands-on experience in:
- Data Engineering  
- ETL/ELT Automation  
- Apache Airflow DAG Design  
- PySpark Data Transformation  
- Data Lake to Warehouse Workflow  
- Local Development Stack with DuckDB  

---
## 📚 Table of Contents

- [📖 Project Overview](#-project-overview)
- [🛠 Tech Stack](#-tech-stack)
- [🏗️ Data Architecture](#-Data-Architecture)
- [🔄 Pipeline Process](#-pipeline-process)
- [🧩 Component Breakdown](#-component-breakdown)
- [📂 Repository Structure](#-repository-structure)
- [📊 Output & Insights](#-output--insights)
- [Setup & Installation](#setup--installation)
- [Usage](#usage)


---
## 🛠 Tech Stack

- **Apache Airflow** – Orchestrates data pipeline execution  
- **PySpark** – Performs data transformation and loading  
- **MinIO** – Acts as a local data lake for raw and clean datasets  
- **DuckDB** – Lightweight analytical data warehouse   
- **Python 3.8+**
  
---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Image](https://github.com/user-attachments/assets/d70f3ca8-637a-4b4f-a12a-b9a96d400205)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into Minio bucket raw.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics. Adding
4. **Airflow**: For orchestrates the ELT pipeline, Supports scheduled and DAG execution

---
### 🔄 Pipeline Process

This project implements a modular, end-to-end **data pipeline** that mirrors real-world data engineering architecture. The process starts from **raw local CSV files**, which are ingested into a **data lake** (MinIO), then transformed using **PySpark** scripts triggered by **Apache Airflow DAGs**, and finally loaded into a **DuckDB-based Data Warehouse** for further modeling and analytics.

Each stage is orchestrated to simulate a scalable and automated ELT (Extract-Load-Transform) workflow.
```
                         ┌──────────────────────────┐
                         │      Source Dataset      │
                         │   (csv di folder local)  │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │     MinIO (Data Lake)    │
                         │   raw/ & clean/ bucket   │
                         └────────────┬─────────────┘
                                      │
                     ┌────────────────┴────────────────┐
                     │                                 │
                     ▼                                 ▼
          ┌────────────────────┐            ┌─────────────────────┐
          │    PySpark (ELT)   │            │     Airflow (DAG)   │
          │ spark/scripts/*.py │◄───────────┤ trigger via DAG run │
          └─────────┬──────────┘            └─────────────────────┘
                    │ 
                    ▼                                    
          ┌────────────────────────────┐
          │   DuckDB (Data Warehouse)  │
          │  duckdb/db/dev.duckdb      │
          └────────────────────────────┘

```
### 🧩 Component Breakdown

Here is a detailed explanation of each component involved in the pipeline:

- **Source Dataset**  
  CSV files containing raw transactional or reference data. These files are stored locally and serve as the starting point of the pipeline.

- **MinIO (Data Lake)**  
  A local object storage system that mimics Amazon S3. It is used to store both raw and cleaned versions of the dataset in separate buckets (`raw/` and `clean/`), ensuring data lineage and accessibility.

- **PySpark (ELT Scripts)**  
  PySpark scripts perform data ingestion from MinIO, cleaning, transformation, and data loading into the warehouse. Located in `spark/scripts/`, these scripts are triggered automatically by Airflow.

- **Airflow (Orchestration/DAG)**  
  Apache Airflow is used to orchestrate the entire workflow. DAGs are defined to automate tasks such as reading from MinIO, running PySpark transformations, and updating the warehouse, enabling full automation and scheduling.

- **DuckDB (Data Warehouse)**  
  A lightweight analytical database used to store transformed data. This acts as the final destination of the pipeline, allowing for fast querying and analytical processing in a local development environment.

---
## 📂 Repository Structure
```
data_engineering/
├── airflow/
│   └──airflow_venv/               ← Airflow virtual environment
│   │    └── airflowhome/          ← Airflow logs, airflows.cfg, airflow.db, webserver_config.py
│   └── dags/                      ← DAG folder for Airflow workflows
│   └── run-airflow.sh             # Script to run Airflow
├── dataset/
│   ├── sources_crm/               ← raw data crm
│   │    └── cust_info.csv/   
│   │    └── prd_info.csv/   
│   │    └── sales_details.csv/   
│   ├── sources_erp/               ← raw data erp
│   │    └── CUST_AZ12.csv/   
│   │    └── LOC_A101.csv/   
│   │    └── PX_CAT_G1V2.csv/  
├── dbt/
│   ├── dbt_venv/                  ← dbt virtual environment
│   ├── logs/                      ← dbt.log
│   └── my_dbt_project/            ← The dbt project contains models, seeds, etc
├── duckdb/
│   └── db/                        ← datawarehouse
├── minio/
│   ├── data/                      # Data folder for MinIO
│   ├── minio                      # Binary MinIO
│   └── run-minio.sh               # Script to run Airflow
├── spark/
│   ├── spark_venv/                # PySpark, JupyterLab, duckdb, ipykernel, numpy, pandas, matplotlib
│   ├── notebooks/                 # .ipynb for ELT, analysis with PySpark/DuckDB
│   ├── scripts/                   # File Python (.py)
│   └── run-jupyter.sh             # Script to run JupyterLab
```

---
## 📊 Output & Insights
This pipeline simulates a real-world batch data pipeline and prepares structured data for further analysis and visualization in tools like Power BI or Metabase.
