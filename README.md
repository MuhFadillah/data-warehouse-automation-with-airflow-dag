# data-warehouse-automation-with-airflow-dag

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
