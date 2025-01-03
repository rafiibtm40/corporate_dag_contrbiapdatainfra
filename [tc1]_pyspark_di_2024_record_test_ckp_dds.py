from google.oauth2 import service_account
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from datetime import datetime, timedelta
from airflow import DAG
from google.cloud import bigquery
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp
from airflow.operators.python import PythonOperator  # <-- Add this import here

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.batamindo_stg_dev.ckp_daily_inbound_2024'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.harvest'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.batamindo_stg_dev.ckp_daily_inbound_2024_err'
STAGING_TABLE = 'biap-datainfra-gcp.batamindo_stg_dev.daily_inbound_staging'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'
LOOKUP_TABLE_ONE = 'biap-datainfra-gcp.ckp_dds.gh_master'
LOOKUP_TABLE_TWO = 'biap-datainfra-gcp.global_dds.harvest_master'
LOOKUP_TABLE_THREE = 'biap-datainfra-gcp.ckp_dds.batch_master'
PROJECT_ID = 'biap-datainfra-gcp'
REGION = 'asia-southeast2'
CLUSTER_NAME = 'dataproc-cluster-{{ ts_nodash }}'  # Adjust dynamically below
PYSPARK_JOB_URI = 'gs://ckp_biap_data/harvest_stg_to_dds_pysparkjob.py'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

# Function to get BigQuery client
def get_bigquery_client(service_account_path):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

# Function to get Spark session
def get_spark_session():
    spark = SparkSession.builder \
        .appName("Airflow_PySpark_ETL") \
        .getOrCreate()
    return spark

# Function to close Spark session
def close_spark_session(spark):
    spark.stop()
    logger.info("Spark session closed.")

# PK and FK Check Logic within PySpark
def pk_fk_check_logic(spark_df, lookup_df_one, lookup_df_two, lookup_df_three):
    from pyspark.sql import functions as F
    
    spark_df = spark_df.withColumnRenamed('sku_name', 'harvest_variant_name') \
        .withColumn('date', to_date(col('date'))) \
        .withColumn('loading_datetime', current_timestamp()) \
        .dropna(subset=['date', 'gh_name', 'harvest_variant_name', 'bruto_kg'])

    spark_df = spark_df.dropDuplicates(['date', 'batch_id', 'gh_name', 'bruto_kg'])

    joined_df = spark_df.join(lookup_df_one, on='gh_name', how='left')
    joined_df = joined_df.join(lookup_df_two, on='harvest_variant_name', how='left')
    joined_df = joined_df.join(lookup_df_three, on='batch_id', how='left')

    valid_data_df = joined_df.filter(
        (col('gh_name').isNotNull()) & 
        (col('harvest_variant_name').isNotNull()) & 
        (col('batch_id').isNotNull())
    )

    invalid_data_df = joined_df.filter(
        (col('gh_name').isNull()) | 
        (col('harvest_variant_name').isNull()) | 
        (col('batch_id').isNull())
    )

    return valid_data_df, invalid_data_df

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dataproc_spark_etl_with_fk_pk_check',
    default_args=default_args,
    description='DAG for submitting PySpark job to Dataproc with PK and FK checks',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Shorten the cluster name dynamically using a timestamp (e.g., 'dataproc-cluster-20241223')
    dynamic_cluster_name = f"dataproc-cluster-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    # Step 1: Create a Dataproc cluster dynamically
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=dynamic_cluster_name,
        cluster_config={  
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": 'n1-standard-4',
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 32
                }
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": 'n1-standard-4',
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 32
                },
                "is_preemptible": False,  # Remove preemptible workers
            },
            "software_config": {
                "image_version": '2.0-debian10',  # Ensure this is compatible with your cluster
                "optional_components": ['JUPYTER']  # Removed SPARK_BIGQUERY
            }
        },
        gcp_conn_id='google_cloud_default',
        timeout=600,
    )

    # Step 2: Initialize Spark session (within task scope)
    def init_spark():
        spark = get_spark_session()
        logger.info("Spark session initialized.")
        return spark

    init_spark_task = PythonOperator(
        task_id='init_spark',
        python_callable=init_spark,
    )

    # Step 3: PK and FK validation logic (submit the PySpark job)
    def run_pk_fk_check():
        spark = get_spark_session()  # Directly create Spark session here
        spark_df = spark.read.option("header", "true").csv(SOURCE_TABLE)
        lookup_df_one = spark.read.option("header", "true").csv(LOOKUP_TABLE_ONE)
        lookup_df_two = spark.read.option("header", "true").csv(LOOKUP_TABLE_TWO)
        lookup_df_three = spark.read.option("header", "true").csv(LOOKUP_TABLE_THREE)

        # Perform PK/FK check logic
        valid_data, invalid_data = pk_fk_check_logic(spark_df, lookup_df_one, lookup_df_two, lookup_df_three)
        
        # Save results to BigQuery
        valid_data.write.format('bigquery').option('table', TARGET_TABLE_PASSED).save()
        invalid_data.write.format('bigquery').option('table', TARGET_TABLE_ERROR).save()

    pk_fk_check_task = PythonOperator(
        task_id='run_pk_fk_check',
        python_callable=run_pk_fk_check,
    )

    # Step 4: Close Spark session after job completion
    def terminate_spark():
        spark = get_spark_session()  # Again, create Spark session within task
        close_spark_session(spark)

    terminate_spark_task = PythonOperator(
        task_id='terminate_spark',
        python_callable=terminate_spark,
    )

    # Step 5: Submit the PySpark job
    submit_spark_job = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        job={  
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": dynamic_cluster_name},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_JOB_URI,
                "args": [SOURCE_TABLE, TARGET_TABLE_PASSED, TARGET_TABLE_ERROR], 
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id='google_cloud_default',
        timeout=600,
    )

    # Step 6: Delete the Dataproc cluster after job completes
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=dynamic_cluster_name,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id='google_cloud_default',
    )

    # Task dependencies: Create cluster -> Initialize Spark -> PK/FK Check -> Close Spark -> Submit Spark Job -> Delete cluster
    create_cluster >> init_spark_task >> pk_fk_check_task >> terminate_spark_task >> submit_spark_job >> delete_cluster
