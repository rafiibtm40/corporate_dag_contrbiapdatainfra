from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.cogs_transaction_partitioned'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.cogs_transaction'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.cogs_transaction_err'
STAGING_TABLE = 'biap-datainfra-gcp.ckp_stg.staging_cogs_transaction'
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
            usage_id, 
            item_name, 
            category_1, 
            category_level_2, 
            usage_location, 
            usage_date, 
            month, 
            quantity_used, 
            units, 
            site, 
            leafy_or_hardy, 
            unit_price, 
            usage_value, 
            currency, 
            kurs, 
            final_usage, 
            location
        FROM 
            {SOURCE_TABLE}
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
    
    # Replace empty strings with np.nan (only in object type columns to avoid errors in boolean or numeric columns)
    for col in transformed_df.select_dtypes(include=['object']).columns:
        transformed_df[col].replace("", np.nan, inplace=True)

    # Add loading_datetime column in GMT+7
    tz = pytz.timezone('Asia/Bangkok')  # GMT+7
    transformed_df['loading_datetime'] = datetime.now(tz)

    # Ensure loading_datetime is in datetime format
    transformed_df['loading_datetime'] = transformed_df['loading_datetime'].apply(lambda x: x.replace(tzinfo=None))

    # Check for records with null required fields and add error handling
    columns_to_check = ['usage_id', 'quantity_used']
    transformed_df = add_null_flags(transformed_df, columns_to_check)

    # Drop rows where any of the required fields (usage_id, quantity_used) are null
    passed_df = transformed_df.dropna(subset=['usage_id', 'quantity_used']) #perlu ditambah Item Name
    
    logger.info(f"Transformed DataFrame shape after dropping nulls: {passed_df.shape}")

    # Add duplicate check flags
    group_columns = ['usage_id', 'quantity_used']
    passed_df = add_duplicate_flags(passed_df, group_columns)

    return passed_df.reset_index(drop=True)

def upsert_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_upsert = ti.xcom_pull(task_ids='transform_data')
    
    if records_to_upsert is None or records_to_upsert.empty:
        logger.warning("No records to upsert.")
        return
    
    client = get_bigquery_client(service_account_path)
    
    # Load the records to be upserted into BigQuery (staging table)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    try:
        client.load_table_from_dataframe(records_to_upsert, STAGING_TABLE, job_config=job_config)
        logger.info(f"Data loaded to staging table {STAGING_TABLE} successfully.")
    except Exception as e:
        logger.error(f"Failed to load data to staging table: {str(e)}")
        return
    
    # Now, run the MERGE query to upsert data into TARGET_TABLE_PASSED
    merge_query = f"""
    MERGE INTO `{TARGET_TABLE_PASSED}` AS target
    USING `{STAGING_TABLE}` AS source
    ON target.usage_id = source.usage_id
    WHEN MATCHED THEN
      UPDATE SET
        target.item_name = source.item_name,
        target.category_1 = source.category_1,
        target.category_level_2 = source.category_level_2,
        target.usage_location = source.usage_location,
        target.usage_date = source.usage_date,
        target.month = source.month,
        target.quantity_used = source.quantity_used,
        target.units = source.units,
        target.site = source.site,
        target.leafy_or_hardy = source.leafy_or_hardy,
        target.unit_price = source.unit_price,
        target.usage_value = source.usage_value,
        target.currency = source.currency,
        target.kurs = source.kurs,
        target.final_usage = source.final_usage,
        target.location = source.location,
        target.loading_datetime = source.loading_datetime
    WHEN NOT MATCHED THEN
      INSERT (usage_id, item_name, category_1, category_level_2, usage_location, usage_date, month, quantity_used, units, site, leafy_or_hardy, unit_price, usage_value, currency, kurs, final_usage, location, loading_datetime)
      VALUES (source.usage_id, source.item_name, source.category_1, source.category_level_2, source.usage_location, source.usage_date, source.month, source.quantity_used, source.units, source.site, source.leafy_or_hardy, source.unit_price, source.usage_value, source.currency, source.kurs, source.final_usage, source.location, source.loading_datetime);
    """
    
    try:
        client.query(merge_query).result()  # Wait for the query to complete
        logger.info(f"Data successfully merged into {TARGET_TABLE_PASSED}.")
    except Exception as e:
        logger.error(f"Error executing MERGE query: {str(e)}")
        
def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    # Fetch the records that have errors (e.g., duplicates, missing fields, etc.)
    error_df = ti.xcom_pull(task_ids='transform_data')

    if error_df is None or error_df.empty:
        logger.warning("No error records to load.")
        return

    # Save the error records into the error table
    client = get_bigquery_client(service_account_path)

    # Load the error data into the error table in BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        client.load_table_from_dataframe(error_df, TARGET_TABLE_ERROR, job_config=job_config)
        logger.info(f"Error data loaded to {TARGET_TABLE_ERROR} successfully.")
    except Exception as e:
        logger.error(f"Failed to load error data to {TARGET_TABLE_ERROR}: {str(e)}")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Bangkok')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    't1_cogs_transaction_dag_dds',
    default_args=default_args,
    description='DAG for processing UOM master data',
    schedule_interval='0 18 1 * *',
    catchup=False,
    tags=['t1_cogs_transaction_dag_table']
)

# Task definitions
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    retries=3,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

upsert_data_task = PythonOperator(
    task_id='upsert_data',
    python_callable=upsert_data,
    dag=dag,
)

load_error_data_task = PythonOperator(
    task_id='load_error_data',
    python_callable=load_error_data,
    dag=dag,
)

# Set task dependencies
extract_data_task >> transform_data_task >> upsert_data_task >> load_error_data_task
