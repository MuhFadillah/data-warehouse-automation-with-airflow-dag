#!/bin/bash

# Aktifkan virtual environment
source airflow_venv/bin/activate

# Set lokasi direktori konfigurasi Airflow
export AIRFLOW_HOME=$(pwd)/airflow_venv/airflowhome

# Jalankan Airflow webserver di port 8080
airflow webserver --port 8080
