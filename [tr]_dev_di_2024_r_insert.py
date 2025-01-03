from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg_dev.daily_inbound_2024'  # Adjust to ckp_dds
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.harvest'  # Adjust to ckp_dds
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2024_err'  # Adjust to ckp_dds
STAGING_TABLE = 'biap-datainfra-gcp.ckp_stg.daily_inbound_staging'  # Adjust to ckp_dds
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'
LOOKUP_TABLE_ONE = 'biap-datainfra-gcp.ckp_dds.gh_master'  # Adjust to ckp_dds
LOOKUP_TABLE_TWO = 'biap-datainfra-gcp.global_dds.harvest_master'  # Adjust to ckp_dds
LOOKUP_TABLE_THREE = 'biap-datainfra-gcp.ckp_dds.batch_master'  # Adjust to ckp_dds

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
            date,
            batch_number,
            gh_name,  
            sku_name,
            harvester_name,
            bruto_kg
        FROM `{SOURCE_TABLE}`
    """
    
    df = client.query(query).to_dataframe()
    logger.info("Data extracted successfully.")
    logger.info(f"Extracted DataFrame shape: {df.shape}")
    return df

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()
    
    # Rename the columns to match the target table schema
    transformed_df.rename(columns={'batch_number': 'batch_id', 'sku_name': 'harvest_variant_name'}, inplace=True)

    # Convert date columns and handle defaults
    transformed_df['date'] = pd.to_datetime(transformed_df['date'], errors='coerce').dt.date  # Convert datetime back to date
    
    # Add loading_datetime column with GMT+7 timezone
    tz = pytz.timezone('Asia/Jakarta')
    transformed_df['loading_datetime'] = datetime.now(tz).replace(tzinfo=None)  # Set as DATETIME

    # Remove records with null PK fields
    transformed_df = transformed_df.dropna(subset=['date', 'batch_id', 'gh_name', 'harvest_variant_name', 'bruto_kg']) 
    logger.info(f"Transformed DataFrame shape after dropping nulls: {transformed_df.shape}")

    # Primary Key check: Remove duplicates (all duplicates based on subset)
    transformed_df = transformed_df[~transformed_df.duplicated(subset=['date', 'batch_id', 'gh_name', 'bruto_kg'], keep='first')]
    logger.info(f"Transformed DataFrame shape after dropping duplicates: {transformed_df.shape}")

    # Group by 'batch_id', 'date', 'gh_name' and sum 'bruto_kg'
    transformed_df = transformed_df.groupby(
        ['date', 'batch_id', 'gh_name'], as_index=False
    ).agg({
        'bruto_kg': 'sum',  # Sum 'bruto_kg' within each group
        'harvest_variant_name': 'first',  # Take the first value for 'harvest_variant_name'
        'loading_datetime': 'first'  # Take the first value for 'loading_datetime'
    })
    logger.info(f"Transformed DataFrame shape after groupby and aggregation: {transformed_df.shape}")

    # FK Checks (Ensuring data integrity via Foreign Keys)
    existing_gh_codes = client.query(f"SELECT gh_code FROM `{LOOKUP_TABLE_ONE}`").to_dataframe()
    valid_gh_codes = transformed_df['gh_name'].isin(existing_gh_codes['gh_code'])
    logger.info(f"Valid gh_codes count: {valid_gh_codes.sum()}")

    existing_varieties = client.query(f"SELECT harvest_variant_name FROM `{LOOKUP_TABLE_TWO}`").to_dataframe()
    valid_varieties = transformed_df['harvest_variant_name'].isin(existing_varieties['harvest_variant_name'])
    logger.info(f"Valid harvest variant names count: {valid_varieties.sum()}")

    existing_batches = client.query(f"SELECT batch_id FROM `{LOOKUP_TABLE_THREE}`").to_dataframe()
    valid_batch_master = transformed_df['batch_id'].isin(existing_batches['batch_id'])
    logger.info(f"Valid batch IDs count: {valid_batch_master.sum()}")

    # Create passed_df based on unique records that passed FK checks
    passed_df = transformed_df[valid_gh_codes & valid_varieties & valid_batch_master].reset_index(drop=True)
    logger.info(f"Passed DataFrame shape: {passed_df.shape}")

    # Create error DataFrame for records that failed FK checks
    invalid_df = transformed_df[~(valid_gh_codes & valid_varieties & valid_batch_master)].reset_index(drop=True)
    error_df = invalid_df.copy()

    # Label the error reasons
    error_df['error_reason'] = ''
    error_df.loc[~valid_gh_codes, 'error_reason'] += 'Invalid GH code; '
    error_df.loc[~valid_varieties, 'error_reason'] += 'Invalid harvest variant name; '
    error_df.loc[~valid_batch_master, 'error_reason'] += 'Invalid batch ID; '
    
    # Additional checks for nulls or missing values
    error_df.loc[error_df['bruto_kg'].isnull(), 'error_reason'] += 'Missing or invalid bruto_kg; '
    error_df.loc[error_df['date'].isnull(), 'error_reason'] += 'Missing date; '
    error_df.loc[error_df['batch_id'].isnull(), 'error_reason'] += 'Missing batch_id; '
    error_df.loc[error_df['gh_name'].isnull(), 'error_reason'] += 'Missing gh_name; '
    error_df.loc[error_df['harvest_variant_name'].isnull(), 'error_reason'] += 'Missing harvest_variant_name; '

    # Add custom error checks
    error_df.loc[error_df['bruto_kg'] < 0, 'error_reason'] += 'Negative bruto_kg; '
    error_df.loc[error_df['date'] < datetime(2020, 1, 1).date(), 'error_reason'] += 'Invalid date; '

    logger.info(f"Error DataFrame shape: {error_df.shape}")

    # Return passed_df and error_df
    return passed_df, error_df

def write_truncate_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_upsert, _ = ti.xcom_pull(task_ids='transform_data')
    
    if records_to_upsert.empty:
        logger.warning("No records to write.")
        return
    
    client = get_bigquery_client(service_account_path)
    
    # Write the passed data to the target table using WRITE_TRUNCATE
    target_table_id = TARGET_TABLE_PASSED
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    try:
        # Load the records to the target table, truncating it first
        client.load_table_from_dataframe(records_to_upsert, target_table_id, job_config=job_config)
        logger.info(f"Data loaded to target table {target_table_id} successfully with WRITE_TRUNCATE.")
    except Exception as e:
        logger.error(f"Failed to load data to target table: {str(e)}")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    logger.info(f"Loading Error DataFrame shape before adjustment: {error_df.shape}")

    # Drop 'batch_id_na' if it exists in error_df
    if 'batch_id_na' in error_df.columns:
        error_df.drop(columns=['batch_id_na'], inplace=True)
        logger.info("Dropped 'batch_id_na' column from error DataFrame.")

    logger.info(f"Loading Error DataFrame shape after adjustment: {error_df.shape}")

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
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Jakarta')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'dev_daily_inbound_24_Write_Truncate',
    default_args=default_args,
    description='DAG for processing daily inbound data',
    schedule_interval='0 18 * * *', 
    catchup=False,
    tags=['dev_daily_inbound_2024']
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

write_truncate_data_task = PythonOperator(
    task_id='write_truncate_data',
    python_callable=write_truncate_data,
    dag=dag,
)

load_error_data_task = PythonOperator(
    task_id='load_error_data',
    python_callable=load_error_data,
    dag=dag,
)

# Set task dependencies
extract_data_task >> transform_data_task >> write_truncate_data_task >> load_error_data_task
