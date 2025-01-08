import os
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from airflow.utils.dates import days_ago, timedelta

# Define service account path
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Set the environment variable for the service account path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

# Define your project details
PROJECT_ID = "biap-datainfra-gcp"
GCS_URI = "gs://bdi_local_stg/gcs_cloudsql_new-postgres-db_01/ckp_batch_master_nh.csv"
INSTANCE_CONNECTION_NAME = "biap-datainfra-gcp:asia-southeast2:new-postgres-db" 
SQL_TABLE = "ckp_batch_master"
SQL_DATABASE = "public"
SQL_USER = "postgres"

# Prepare the body for the import request
import_body = {
    "importContext": {
        "uri": GCS_URI,
        "fileType": "CSV",
        "csvImportOptions": {
            "table": SQL_TABLE,
            "columns": ["batch_id", "gh_name", 'vegetable_variety', 'batch_start_date', 'transplant_date', 'batch_end_date', 'original_population', 'loading_datetime']
        },
        "database": SQL_DATABASE,
        "importUser": SQL_USER,
    }
}

# Define the DAG arguments and schedule
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_to_cloudsql',
    default_args=default_args,
    description='Directly transfer data from GCS to CloudSQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task: Use CloudSQLImportInstanceOperator to import data from GCS to CloudSQL
    push_to_cloudsql = CloudSQLImportInstanceOperator(
        task_id='push_to_cloudsql',
        body=import_body,
        instance=INSTANCE_CONNECTION_NAME,  # Correct instance connection name
        project_id=PROJECT_ID
    )

    # Define task dependencies
    push_to_cloudsql
