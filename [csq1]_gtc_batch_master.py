from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSToCloudSQLOperator
from airflow.providers.google.cloud.transfers.gcs import GCSFileToLocalOperator
from airflow.providers.postgres.transfers.gcs import GCSToPostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define your project details
PROJECT_ID = "biap-datainfra-gcp"
GCS_BUCKET = "bdi_local_stg"
GCS_URI = "gs://bdi_local_stg/gcs_cloudsql_new-postgres-db_01"
CLOUDSQL_CONN_ID = "google_cloud_default"
SQL_TABLE = "ckp_batch_msater"
SQL_QUERY = "COPY your_table_name FROM STDIN WITH CSV HEADER"  # Modify the query based on your needs

# Define the DAG arguments and schedule
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_to_cloudsql',
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
        object_name='gcs_cloudsql_new-postgres-db_01',  # Adjust file path if needed
        filename='/tmp/data_to_load.csv',  # Local file path to save data
        google_cloud_storage_conn_id='google_cloud_default',
    )

    # Task 2: Push data from local to CloudSQL PostgreSQL
    push_to_cloudsql = GCSToCloudSQLOperator(
        task_id='push_to_cloudsql',
        bucket_name=GCS_BUCKET,
        object_name='gcs_cloudsql_new-postgres-db_01',
        sql_database='your_database',  # CloudSQL database name
        sql_table=SQL_TABLE,           # Target table in CloudSQL
        sql_query=SQL_QUERY,           # SQL COPY command for PostgreSQL
        sql_conn_id=CLOUDSQL_CONN_ID,  # Airflow CloudSQL connection ID
        google_cloud_storage_conn_id='google_cloud_default',
    )

    # Define task dependencies
    pull_gcs_data >> push_to_cloudsql
