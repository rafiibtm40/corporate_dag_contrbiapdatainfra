from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define your project details
PROJECT_ID = "biap-datainfra-gcp"
GCS_BUCKET = "bdi_local_stg"
GCS_URI = "gs://bdi_local_stg/gcs_cloudsql_new-postgres-db_01/ckp_batch_master_nh.csv"  # Updated GCS URI
CLOUDSQL_CONN_ID = "google_cloud_default"
SQL_TABLE = "ckp_batch_master"
SQL_QUERY = "COPY ckp_batch_master FROM 'gs://bdi_local_stg/gcs_cloudsql_new-postgres-db_01/ckp_batch_master_nh.csv' WITH CSV;"  # Adjusted COPY query for the specific file

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
    schedule_interval='@daily',  # Daily execution
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task: Use PostgresOperator to directly push data from GCS to CloudSQL
    push_to_cloudsql = PostgresOperator(
        task_id='push_to_cloudsql',
        postgres_conn_id=CLOUDSQL_CONN_ID,  # Airflow Postgres connection ID
        sql=SQL_QUERY,  # SQL COPY command for PostgreSQL
    )

    # Define task dependencies
    push_to_cloudsql  # Single task, no dependencies
