from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2024' #adjust to dds_dev
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.harvest_test' #adjust to ckp_dds
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2024_err' 
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'
LOOKUP_TABLE_ONE = 'biap-datainfra-gcp.ckp_dds.gh_master' #adjust to ckp_dds
LOOKUP_TABLE_TWO = 'biap-datainfra-gcp.global_dds.harvest_master' #adjust to global_dds
LOOKUP_TABLE_THREE = 'biap-datainfra-gcp.ckp_dds.batch_master' #adjust to ckp_dds

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    client = get_bigquery_client(service_account_path)
    query = f"""
        WITH ranked_data AS (
        SELECT
            d1.date,
            d1.gh_name,
            d1.sku_name,
            d1.bruto_kg,
            bm.batch_id AS batch_number,
            bm.vegetable_variety,
            bm.batch_start_date,
            bm.batch_end_date,
            ROW_NUMBER() OVER (
            PARTITION BY d1.date, bm.batch_id, d1.gh_name, d1.sku_name, CAST(d1.bruto_kg AS STRING)
            ORDER BY d1.date ASC
            ) AS row_num
        FROM (
            SELECT
            di.date,
            di.gh_name,
            di.sku_name,
            di.bruto_kg,
            hm.vegetable_variety
            FROM `{SOURCE_TABLE}` AS di
            LEFT JOIN `{LOOKUP_TABLE_TWO}` AS hm
            ON di.sku_name = hm.harvest_variant_name
            WHERE di.sku_name IS NOT NULL
        ) AS d1
        LEFT JOIN `{LOOKUP_TABLE_THREE}` AS bm
            ON d1.gh_name = bm.gh_name
            AND d1.vegetable_variety = bm.vegetable_variety 
            AND d1.date BETWEEN bm.batch_start_date AND bm.batch_end_date
        WHERE d1.sku_name IS NOT NULL
        )

        SELECT
            date,
            gh_name,
            sku_name,
            SUM(bruto_kg) AS bruto_kg,
            batch_number,
            vegetable_variety,
            batch_start_date,
            batch_end_date
        FROM ranked_data
        WHERE row_num = 1
        GROUP BY
        gh_name,
            date,
            sku_name,
            batch_number,
            vegetable_variety,
            batch_start_date,
            batch_end_date
    """

    df = client.query(query).to_dataframe()
    df = df.drop(columns=['batch_start_date', 'batch_end_date'])
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
    transformed_df.rename(columns={'batch_number': 'batch_id', 'sku_name': 'harvest_variant_name'}, inplace=True)

    # Convert date columns and handle defaults
    transformed_df['date'] = pd.to_datetime(transformed_df['date'], errors='coerce').dt.date
    
    # Add loading_datetime column
    tz = pytz.timezone('Asia/Jakarta')
    transformed_df['loading_datetime'] = datetime.now(tz).replace(tzinfo=None)
    
    # Remove records with null PK fields
    transformed_df = transformed_df.dropna(subset=['date', 'batch_id', 'gh_name', 'harvest_variant_name', 'bruto_kg'])
    logger.info(f"Transformed DataFrame shape after dropping nulls: {transformed_df.shape}")

    # Check for duplicates on primary key fields
    transformed_df = transformed_df[~transformed_df.duplicated(subset=['date', 'batch_id', 'gh_name','bruto_kg'], keep=False)]
    logger.info(f"Transformed DataFrame shape after dropping duplicates: {transformed_df.shape}")

    # FK Checks
    existing_gh_codes = client.query(f"SELECT gh_code FROM `{LOOKUP_TABLE_ONE}`").to_dataframe()
    valid_gh_codes = transformed_df['gh_name'].isin(existing_gh_codes['gh_code'])
    
    existing_varieties = client.query(f"SELECT harvest_variant_name FROM `{LOOKUP_TABLE_TWO}`").to_dataframe()
    valid_varieties = transformed_df['harvest_variant_name'].isin(existing_varieties['harvest_variant_name'])

    existing_batches = client.query(f"SELECT batch_id FROM `{LOOKUP_TABLE_THREE}`").to_dataframe()
    valid_batch_master = transformed_df['batch_id'].isin(existing_batches['batch_id'])

    # Create passed_df based on unique records that passed FK checks
    passed_df = transformed_df[valid_gh_codes & valid_varieties & valid_batch_master].reset_index(drop=True)
    logger.info(f"Passed DataFrame shape: {passed_df.shape}")

    # Create error DataFrame for records that failed FK checks
    invalid_df = transformed_df[~(valid_gh_codes & valid_varieties & valid_batch_master)].reset_index(drop=True)
    error_df = invalid_df.copy()
    
    logger.info(f"Error DataFrame shape: {error_df.shape}")

    # Return passed_df and error_df
    return passed_df, error_df

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, _ = ti.xcom_pull(task_ids='transform_data')
    
    client = get_bigquery_client(service_account_path)
    
    logger.info(f"Records to insert: {records_to_insert.shape[0]}")
    
    if not records_to_insert.empty:
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
    'new_insert_daily_inbound_24',
    default_args=default_args,
    description='DAG for processing daily inbound data',
    schedule_interval=None,  # No automatic schedule
    catchup=False,  # No backfilling
    tags=['dds_harvest_test_daily_inbound_2024']
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
