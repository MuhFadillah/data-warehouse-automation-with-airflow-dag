#!/bin/bash

# Enable virtual environment
source airflow_venv/bin/activate

# Set the location of the Airflow configuration directory
export AIRFLOW_HOME=$(pwd)/airflow_venv/airflowhome

# Run Airflow webserver on port 8080
airflow webserver --port 8080
