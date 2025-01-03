from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import logging
import pytz
import os

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.mitra_group_database'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.mitra_group_database'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.mitra_group_database_err'
SERVICE_ACCOUNT_PATH = os.getenv('SERVICE_ACCOUNT_PATH', '/home/corporate/myKeys/airflowbiapvm.json')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    """Get a BigQuery client using the service account credentials."""
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    """Extract data from BigQuery and return it as a DataFrame."""
    client = get_bigquery_client(service_account_path)
    query = f"""
        SELECT 
            id_farmer,
            farmer_name,
            assign_group,
            transplant_date,
            end_date
        FROM `{SOURCE_TABLE}`
    """
    
    df = client.query(query).to_dataframe()
    logger.info(f"Data extracted successfully. Shape: {df.shape}")
    return df

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    """Transform the extracted data, validate, and separate it into passed and error DataFrames."""
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()
    
    # Step 1: Data validation and cleaning
    transformed_df['id_farmer_na'] = transformed_df['id_farmer'].isnull().astype(int)
    transformed_df['farmer_name_na'] = transformed_df['farmer_name'].isnull().astype(int)
    transformed_df['assign_group_na'] = transformed_df['assign_group'].isnull().astype(int)
    
    # Step 2: Check for duplicates on primary key fields (farmer_name, assign_group)
    transformed_df['duplicate_flag'] = transformed_df.duplicated(subset=['id_farmer','farmer_name', 'assign_group'], keep=False).astype(int)
    
    # Step 3: Create the error DataFrame for rows with nulls or duplicates
    error_df = transformed_df[transformed_df[['id_farmer_na', 'farmer_name_na', 'assign_group_na']].sum(axis=1) > 0]
    
    # Step 4: Separate valid and error records
    passed_df = transformed_df[transformed_df['duplicate_flag'] == 0]
    passed_df = passed_df.reset_index(drop=True)
    error_df = error_df.reset_index(drop=True)

    logger.info(f"Passed DataFrame shape: {passed_df.shape}")
    logger.info(f"Error DataFrame shape: {error_df.shape}")

    # Step 5: Add the 'loading_datetime' to passed data
    passed_df['loading_datetime'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Ensure proper format for date columns (e.g., transplant_date, end_date)
    passed_df['transplant_date'] = pd.to_datetime(passed_df['transplant_date'], errors='coerce')
    passed_df['end_date'] = pd.to_datetime(passed_df['end_date'], errors='coerce')

    # Drop temporary columns like 'farmer_name_null_flag', 'assign_group_null_flag', 'duplicate_flag'
    target_columns = ['id_farmer','farmer_name', 'assign_group','transplant_date','end_date','loading_datetime']
    passed_df = passed_df[target_columns]
    
    logger.info(f"Passed DataFrame columns after cleanup: {passed_df.columns}")
    
    # Add 'loading_datetime' to error DataFrame
    error_df['loading_datetime'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Error DataFrame should include flags for farmer_name and assign_group null and duplicate checks
    error_df = error_df[['id_farmer', 'farmer_name', 'assign_group','transplant_date','end_date','loading_datetime']]
    error_df['farmer_id_na'] = error_df['id_farmer'].isnull().astype(int)
    error_df['assign_group_na'] = error_df['assign_group'].isnull().astype(int)
    error_df['farmer_name_na'] = error_df['farmer_name'].isnull().astype(int)
    error_df['duplicate_flag'] = error_df.duplicated(subset=['farmer_name', 'assign_group'], keep=False).astype(int)
    
    return passed_df, error_df

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    """Load the valid (passed) data into the target BigQuery table."""
    ti = kwargs['ti']
    passed_df, _ = ti.xcom_pull(task_ids='transform_data')
    
    client = get_bigquery_client(service_account_path)
    
    if not passed_df.empty:
        try:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(passed_df, TARGET_TABLE_PASSED, job_config=job_config)
            logger.info(f"Inserted {passed_df.shape[0]} records into passed table.")
        except Exception as e:
            logger.error(f"Failed to insert records into passed table: {str(e)}")
    else:
        logger.warning("No records to load into passed table.")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    """Load the error data into the target BigQuery error table."""
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')
    
    client = get_bigquery_client(service_account_path)
    
    if not error_df.empty:
        try:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(error_df, TARGET_TABLE_ERROR, job_config=job_config)
            logger.info(f"Loaded {error_df.shape[0]} error records.")
        except Exception as e:
            logger.error(f"Failed to load error records: {str(e)}")
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
    'mitra_group_database_dag',
    default_args=default_args,
    description='DAG for processing daily inbound data',
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['dev_ckp_master_table']
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
