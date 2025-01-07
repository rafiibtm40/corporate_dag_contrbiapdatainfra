from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.cogs_uom_master'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.cogs_uom_master'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.cogs_uom_master_err'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

# Function to create null check columns for multiple columns
def add_null_flags(df, columns):
    for col in columns:
        df[f'{col}_null_flag'] = df[col].isnull().astype(int)
    return df

# Function to create duplicate check flags
def add_duplicate_flags(df, group_columns):
    duplicate_counts = df.groupby(group_columns).size().reset_index(name='count')
    df = df.merge(duplicate_counts[group_columns + ['count']], on=group_columns, how='left')
    df['duplicate_flag'] = df['count'].apply(lambda x: 1 if x > 1 else 0)
    df = df.drop(columns=['count'])
    return df

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    client = get_bigquery_client(service_account_path)
    query = f"""
        SELECT 
            unit_id,
            unit_name_yes,
            unit_name_plural_yes,
            unit_name_conv,
            unit_name_plural_conv,
            based_conv,
            rate_conv
        FROM `{SOURCE_TABLE}` 
    """
    
    try:
        df = client.query(query).to_dataframe()
        logger.info("Data extracted successfully.")
        logger.info(f"Extracted DataFrame shape: {df.shape}")
        logger.info(f"Extracted Data: \n{df.head()}")
        return df
    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}")
        raise

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()

    # Add loading_datetime column in GMT+7
    tz = pytz.timezone('Asia/Bangkok')  # GMT+7
    transformed_df['loading_datetime'] = datetime.now(tz)

    # Ensure loading_datetime is in datetime format
    transformed_df['loading_datetime'] = pd.to_datetime(transformed_df['loading_datetime'])

    # Check for records with null required fields and add error handling
    columns_to_check = ['unit_id']
    transformed_df = add_null_flags(transformed_df, columns_to_check)

    # Identify error records (i.e., rows with null flags) - Explicitly use unit_id_null_flag
    error_df = transformed_df[transformed_df['unit_id_null_flag'] == 1].reset_index(drop=True)
    
    # Drop rows where any of the required fields (unit_id) are null
    passed_df = transformed_df.dropna(subset=['unit_id'])
    
    logger.info(f"Transformed DataFrame shape after dropping nulls: {passed_df.shape}")

    if passed_df.empty:
        logger.info("No valid records left after NaN removal.")
    
    # Add duplicate check flags (grouping by unit_id and other columns)
    group_columns = ['unit_id', 'unit_name_yes', 'unit_name_plural_yes','based_conv','rate_conv','unit_name_conv','unit_name_plural_conv']
    passed_df = add_duplicate_flags(passed_df, group_columns)
    
    # Add error handling for duplicates
    duplicate_error_df = passed_df[passed_df['duplicate_flag'] == 1].reset_index(drop=True)  # Look for flag 1 (duplicates)
    if not duplicate_error_df.empty:
        logger.info(f"Duplicate records detected: {duplicate_error_df.shape[0]}")
        duplicate_error_df['error_type'] = 'Duplicate Column'
        duplicate_error_df['error_description'] = 'Duplicate records based on the specified columns.'
        duplicate_error_df['flagging'] = 2  # Set flagging to 2 for duplicate errors
        duplicate_error_df = duplicate_error_df.drop(columns=['duplicate_flag'])

    # Prepare error records for missing unit_id (Missing Required Fields)
    if not error_df.empty:
        logger.info(f"Missing unit_id records: {error_df.shape[0]}")
        error_df['error_type'] = 'Missing Required Fields'
        error_df['error_description'] = 'One or more required fields are missing.'
        error_df['flagging'] = 1  # Set flagging to 1 for missing required field errors
        error_df['unit_id_na'] = error_df['unit_id_null_flag']
        error_df = error_df.drop(columns=['unit_id_null_flag'], errors='ignore')

    # Combine missing required fields and duplicate error records
    full_error_df = pd.concat([error_df, duplicate_error_df]).reset_index(drop=True)
    
    # Ensure passed_df is not empty
    if passed_df.empty:
        logger.warning("No records passed after transformation.")
    
    # Remove count_pk from passed_df, it should not be added here
    passed_df = passed_df.drop(columns=['count_pk'], errors='ignore')

    # Drop the unit_id_null_flag from passed_df to ensure it is not included in the passed data table
    passed_df = passed_df.drop(columns=['unit_id_null_flag','duplicate_flag'], errors='ignore')

    return passed_df.reset_index(drop=True), full_error_df.reset_index(drop=True)

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, _ = ti.xcom_pull(task_ids='transform_data')
    
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
        
        # Load the new records
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(records_to_insert, TARGET_TABLE_PASSED, job_config=job_config).result()
        logger.info("Inserted new records successfully.")
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    if error_df is None or error_df.empty:
        logger.warning("No error data to load.")
        return
    
    client = get_bigquery_client(service_account_path)

    logger.info(f"Loading Error DataFrame shape before adjustment: {error_df.shape}")

    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(error_df, TARGET_TABLE_ERROR, job_config=job_config).result()
        logger.info("Error data loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load error data: {str(e)}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Bangkok')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    't1_cogs_uom_master_dag_dds',
    default_args=default_args,
    description='DAG for processing UOM master data',
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['t1_ckp_uom_master_table']
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
