from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2025'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.harvest'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2025_err'
STAGING_TABLE = 'biap-datainfra-gcp.ckp_stg.daily_inbound_staging'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'
LOOKUP_TABLE_ONE = 'biap-datainfra-gcp.ckp_dds.gh_master'
LOOKUP_TABLE_TWO = 'biap-datainfra-gcp.global_dds.harvest_master'
LOOKUP_TABLE_THREE = 'biap-datainfra-gcp.ckp_dds.batch_master'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    try:
        # Initialize BigQuery client with the service account
        client = get_bigquery_client(service_account_path)
        
        # Define the query to extract data
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
        # Log the query to make sure it's correct
        logger.info(f"Running query: {query}")
        
        # Execute the query and convert result to DataFrame
        df = client.query(query).to_dataframe()
        
        # Log the DataFrame shape to verify data is extracted
        logger.info(f"Data extracted, shape: {df.shape}")
        
        # Check if the DataFrame is empty
        if df.empty:
            logger.warning("Extracted data is empty!")
            return None
        
        return df
    
    except Exception as e:
        logger.error(f"Error in extract_data: {str(e)}")
        raise

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
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
    transformed_df = transformed_df.dropna(subset=['date', 'gh_name', 'harvest_variant_name', 'bruto_kg'])
    logger.info(f"Transformed DataFrame shape after dropping nulls: {transformed_df.shape}")

    # Primary Key check: Remove duplicates (all duplicates based on subset)
    transformed_df = transformed_df[~transformed_df.duplicated(subset=['date', 'batch_id', 'gh_name', 'bruto_kg'], keep='first')]

    logger.info(f"Transformed DataFrame shape after dropping duplicates: {transformed_df.shape}")

    # Group by 'batch_id', 'date', 'gh_name' and sum 'bruto_kg'
    transformed_df = transformed_df.groupby(
        ['date', 'batch_id', 'gh_name'], as_index=False
    ).agg({
        'bruto_kg': 'sum',  
        'harvest_variant_name': 'first',  
        'loading_datetime': 'first'  
    })
    logger.info(f"Transformed DataFrame shape after groupby and aggregation: {transformed_df.shape}")

    # FK Checks (Ensuring data integrity via Foreign Keys)
    client = get_bigquery_client(service_account_path)
    
    # Lookup data for FK checks
    existing_gh_codes = client.query(f"SELECT gh_code FROM `{LOOKUP_TABLE_ONE}`").to_dataframe()
    existing_varieties = client.query(f"SELECT harvest_variant_name FROM `{LOOKUP_TABLE_TWO}`").to_dataframe()
    existing_batches = client.query(f"SELECT batch_id FROM `{LOOKUP_TABLE_THREE}`").to_dataframe()

    # Apply FK validation
    valid_gh_codes = transformed_df['gh_name'].isin(existing_gh_codes['gh_code'])
    valid_varieties = transformed_df['harvest_variant_name'].isin(existing_varieties['harvest_variant_name'])
    valid_batch_master = transformed_df['batch_id'].isin(existing_batches['batch_id'])

    # Create passed_df based on valid FK checks
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
    error_df.loc[error_df['bruto_kg'].isnull(), 'error_reason'] += 'Missing bruto_kg; '
    error_df.loc[error_df['date'].isnull(), 'error_reason'] += 'Missing date; '
    error_df.loc[error_df['batch_id'].isnull(), 'error_reason'] += 'Missing batch_id; '
    error_df.loc[error_df['gh_name'].isnull(), 'error_reason'] += 'Missing gh_name; '
    error_df.loc[error_df['harvest_variant_name'].isnull(), 'error_reason'] += 'Missing harvest_variant_name; '

    logger.info(f"Error DataFrame shape: {error_df.shape}")

    return passed_df, error_df

def upsert_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_upsert, _ = ti.xcom_pull(task_ids='transform_data')
    
    if records_to_upsert is None or records_to_upsert.empty:
        logger.warning("No records to upsert.")
        return
    
    client = get_bigquery_client(service_account_path)
    
    # Load the records to be upserted into BigQuery (staging table)
    staging_table_id = STAGING_TABLE
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    try:
        client.load_table_from_dataframe(records_to_upsert, staging_table_id, job_config=job_config)
        logger.info(f"Data loaded to staging table {staging_table_id} successfully.")
    except Exception as e:
        logger.error(f"Failed to load data to staging table: {str(e)}")
        return
    
    # Now, run the MERGE query to upsert data into TARGET_TABLE_PASSED
    merge_query = f"""
    MERGE INTO `{TARGET_TABLE_PASSED}` AS target
    USING `{STAGING_TABLE}` AS source
    ON target.date = source.date
      AND target.batch_id = source.batch_id
      AND target.gh_name = source.gh_name
    WHEN MATCHED THEN
      UPDATE SET
        target.bruto_kg = source.bruto_kg,
        target.harvest_variant_name = source.harvest_variant_name,
        target.loading_datetime = source.loading_datetime
    WHEN NOT MATCHED THEN
      INSERT (date, batch_id, gh_name, bruto_kg, harvest_variant_name, loading_datetime)
      VALUES (source.date, source.batch_id, source.gh_name, source.bruto_kg, source.harvest_variant_name, source.loading_datetime);
    """
    
    try:
        client.query(merge_query).result()  # Wait for the query to complete
        logger.info(f"Data successfully merged into {TARGET_TABLE_PASSED}.")
    except Exception as e:
        logger.error(f"Error executing MERGE query: {str(e)}")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    if not error_df.empty:
        # Rename 'batch_id' to 'batch_number'
        error_df = error_df.rename(columns={'batch_id': 'batch_number'})

        # Add missing columns for the error table
        error_df['error_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Load data to error table
        try:
            table_id = TARGET_TABLE_ERROR
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(error_df, table_id, job_config=job_config)
            logger.info(f"Error data loaded to {TARGET_TABLE_ERROR}.")
        except Exception as e:
            logger.error(f"Error loading data into {TARGET_TABLE_ERROR}: {str(e)}")
    else:
        logger.warning("No records in error data.")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 1),
}

dag = DAG(
    'upsert_daily_inbound_25',
    default_args=default_args,
    description='Upsert and load data into BigQuery with error handling',
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['upset_harvest_to_dds_ckp']
)

# Task Definitions
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

upsert_data_task = PythonOperator(
    task_id='upsert_data',
    python_callable=upsert_data,
    provide_context=True,
    dag=dag,
)

load_error_data_task = PythonOperator(
    task_id='load_error_data',
    python_callable=load_error_data,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
extract_data_task >> transform_data_task >> [upsert_data_task, load_error_data_task]
