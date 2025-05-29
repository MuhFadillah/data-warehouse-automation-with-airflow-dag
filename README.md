# Data Warehouse Automation With Airflow DAG Project

Welcome to the **Data Warehouse Automation With Airflow DAG Project (Data Engineer)** repository! 🚀  
This project showcases a complete data engineering workflow from ingesting raw data into a Data Lake, transforming it via PySpark, orchestrating pipelines with Apache Airflow, to storing structured data in a Data Warehouse for analytics.  

---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Image](https://github.com/user-attachments/assets/8621fdbb-42d7-47b7-934d-d76b248c66fb)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into Minio bucket raw.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics. Adding
4. **Airflow**: For orchestrates the ELT pipeline, Supports scheduled and DAG execution

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
