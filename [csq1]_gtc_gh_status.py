from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs import GCSToCloudSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define your project details
PROJECT_ID = "biap-datainfra-gcp"
GCS_BUCKET = "bdi_local_stg"
GCS_URI = "gs://bdi_local_stg/gcs_cloudsql_new-postgres-db_01/gh_status_nh.csv"  # Updated GCS URI
CLOUDSQL_CONN_ID = "google_cloud_default"
SQL_TABLE = "ckp_gh_status"
SQL_QUERY = "COPY ckp_gh_status FROM 'gs://bdi_local_stg/gcs_cloudsql_new-postgres-db_01/gh_status_nh.csv' WITH CSV;"  # Adjusted COPY query for the specific file

# Define the DAG arguments and schedule
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_to_cloudsql_gh_status',
    default_args=default_args,
    description='Pull data from GCS and load to CloudSQL',
    schedule_interval='@daily',  # Daily execution
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Pull data from GCS to a local file
    pull_gcs_data = GCSFileToLocalOperator(
        task_id='pull_gcs_data',
        bucket_name=GCS_BUCKET,
        object_name='gcs_cloudsql_new-postgres-db_01/gh_status_nh.csv',  # Updated path for file
        filename='/tmp/data_to_load.csv',  # Local file path to save data
        google_cloud_storage_conn_id='google_cloud_default',
    )

    # Task 2: Push data from local to CloudSQL (Postgres)
    push_to_cloudsql = PostgresOperator(
        task_id='push_to_cloudsql',
        postgres_conn_id=CLOUDSQL_CONN_ID,  # Airflow Postgres connection ID
        sql=SQL_QUERY,  # Updated SQL query to use the specific file
    )

    # Define task dependencies
    pull_gcs_data >> push_to_cloudsql
