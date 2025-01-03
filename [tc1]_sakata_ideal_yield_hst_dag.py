from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.global_stg.sakata_hst_ideal_yield'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.global_dds.sakata_hst_ideal_yield'
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
            days_from_transplant,
            gh_yield,
            cumulative_yield
        FROM `{SOURCE_TABLE}`
    """
    
    try:
        df = client.query(query).to_dataframe()
        logger.info("Data extracted successfully.")
        logger.info(f"Extracted DataFrame shape: {df.shape}")
        logger.info(f"Extracted Data: \n{df.head()}")
        return df
    except Exception as e:
        logger.error(f"Failed to extract data from BigQuery: {str(e)}")
        raise

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()
    
    # Add loading_datetime column in GMT+7
    tz = pytz.timezone('Asia/Bangkok')  # GMT+7
    transformed_df['loading_datetime'] = datetime.now(tz)

    # Ensure loading_datetime is in datetime format
    transformed_df['loading_datetime'] = pd.to_datetime(transformed_df['loading_datetime'])
    
    # Here you can perform any additional transformations needed, e.g., filtering or adding new columns
    
    # Return transformed data for the load task
    return transformed_df  # This should be returned for the next task

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert = ti.xcom_pull(task_ids='transform_data')
    
    if records_to_insert is None or records_to_insert.empty:
        logger.warning("No records to insert into passed table.")
        return

    client = get_bigquery_client(service_account_path)
    
    logger.info(f"Records to insert: {records_to_insert.shape[0]}")
    
    try:
        # Truncate the target table before loading new data
        truncate_query = f"TRUNCATE TABLE `{TARGET_TABLE_PASSED}`"
        client.query(truncate_query).result()
        logger.info(f"Truncated table {TARGET_TABLE_PASSED} successfully.")
        
        # Load the new records into the passed table (including loading_datetime)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(records_to_insert, TARGET_TABLE_PASSED, job_config=job_config)
        logger.info("Inserted new records successfully.")
    except Exception as e:
        logger.error(f"Failed to insert new records into {TARGET_TABLE_PASSED}: {str(e)}")
        raise

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Bangkok')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    't1_sakata_ideal_yield_hst_dag',
    default_args=default_args,
    description='DAG for processing sakata_ideal_yield_hst',
    schedule_interval='0 18 * * *',  # Daily at 18:00
    catchup=False,
    tags=['t1_ideal_yield']
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

# Set task dependencies
extract_data_task >> transform_data_task >> load_passed_data_task
