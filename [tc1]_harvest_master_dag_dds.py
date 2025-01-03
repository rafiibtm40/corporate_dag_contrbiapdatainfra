from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.batamindo_stg_dev.ckp_harvest_master'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.global_dds.harvest_master'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.batamindo_stg_dev.ckp_harvest_master_err'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    client = get_bigquery_client(service_account_path)
    query = f"""
        SELECT 
            sku_code,
            sku_variant_name,
            sku_name,
            sku_other_name,
            vegetable_variant,
            sku_category,
            source,
            new_code
        FROM `{SOURCE_TABLE}`
    """
    
    df = client.query(query).to_dataframe()
    logger.info("Data extracted successfully.")
    logger.info(f"Extracted DataFrame shape: {df.shape}")
    logger.info(f"Extracted Data: \n{df.head()}")
    return df

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()
    
    # Rename the columns to match the target table schema
    transformed_df.rename(columns={
        'sku_code': 'harvest_code',
        'sku_variant_name': 'harvest_variant_name',
        'sku_name': 'harvest_name',
        'vegetable_variant': 'vegetable_variety',
        'sku_other_name': 'harvest_other_name',
        'sku_category': 'vegetable_category',
        'source': 'vegetable_in_house',
    }, inplace=True)

    # Add loading_datetime column in GMT+7
    tz = pytz.timezone('Asia/Bangkok')  # GMT+7
    transformed_df['loading_datetime'] = datetime.now(tz)  # Set as DATETIME

    # Include new_code
    transformed_df['new_code'] = df['new_code']

    # Remove records with null required fields
    transformed_df = transformed_df.dropna(subset=[
        'harvest_code', 'harvest_variant_name', 
        'harvest_name', 'harvest_other_name', 
        'vegetable_category'
    ])
    logger.info(f"Transformed DataFrame shape after dropping nulls: {transformed_df.shape}")

    # Check for duplicates on primary key fields
    transformed_df = transformed_df[~transformed_df.duplicated(subset=['harvest_code', 'harvest_variant_name'], keep='first')]
    logger.info(f"Transformed DataFrame shape after dropping duplicates: {transformed_df.shape}")

    # Split into passed and error DataFrames
    passed_df = transformed_df.reset_index(drop=True)
    error_df = transformed_df[transformed_df.isnull().any(axis=1)].reset_index(drop=True)

    logger.info(f"Passed DataFrame shape: {passed_df.shape}")
    logger.info(f"Error DataFrame shape: {error_df.shape}")

    # Return passed_df and error_df
    return passed_df, error_df

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, _ = ti.xcom_pull(task_ids='transform_data')
    
    client = get_bigquery_client(service_account_path)
    
    logger.info(f"Records to insert: {records_to_insert.shape[0]}")
    
    if not records_to_insert.empty:
        # Truncate the target table before loading new data
        truncate_query = f"TRUNCATE TABLE `{TARGET_TABLE_PASSED}`"
        try:
            client.query(truncate_query).result()
            logger.info(f"Truncated table {TARGET_TABLE_PASSED} successfully.")
        except Exception as e:
            logger.error(f"Failed to truncate table {TARGET_TABLE_PASSED}: {str(e)}")
            return

        # Load the new records
        try:
            client.load_table_from_dataframe(records_to_insert, TARGET_TABLE_PASSED, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
            logger.info("Inserted new records successfully.")
        except Exception as e:
            logger.error(f"Failed to insert new records: {str(e)}")
    else:
        logger.warning("No records to insert.")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    logger.info(f"Loading Error DataFrame shape before adjustment: {error_df.shape}")

    if not error_df.empty:
        try:
            table_id = TARGET_TABLE_ERROR
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(error_df, table_id, job_config=job_config)
            logger.info("Error data loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load error data: {str(e)}")
    else:
        logger.info("No error data to load.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Bangkok')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'tc1_harvest_master_dag_dds',
    default_args=default_args,
    description='DAG for processing daily inbound data',
    schedule_interval='0 18 * * *', # will be triggered 01.00 AM GMT+7
    catchup=False,
    tags=['t1_ckp_harvest_master_table']
)

# Task definitions
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_passed_data_task = PythonOperator(
    task_id='load_passed_data',
    python_callable=load_passed_data,
    dag=dag,
)

load_error_data_task = PythonOperator(
    task_id='load_error_data',
    python_callable=load_error_data,
    dag=dag,
)

# Set task dependencies
extract_data_task >> transform_data_task >> [load_passed_data_task, load_error_data_task]
