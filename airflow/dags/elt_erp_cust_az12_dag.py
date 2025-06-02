from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
}

with DAG(
    dag_id='elt_erp_cust_az12_dag',
    default_args=default_args,
    schedule_interval=None,  # Ubah ke '0 1 * * *' jika mau dijalankan otomatis harian jam 1 pagi
    catchup=False,
    description='DAG untuk menjalankan ELT ERP Cust az12 via PySpark dan MinIO',
    tags=['elt', 'spark', 'minio'],
) as dag:

    run_elt_job = BashOperator(
        task_id='run_elt_crm_job',
        bash_command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 /mnt/d/data_engineering/spark/scripts/elt_erp_cust_az12.py'
    )
