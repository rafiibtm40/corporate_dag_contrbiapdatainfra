from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2024'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.harvest'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.daily_inbound_2024_err'
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

def extract_data(**kwargs):
    client = get_bigquery_client()
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
    logger.info(f"Extracted {df.shape[0]} records.")
    return df

def transform_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data')
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    # Data transformation
    transformed_df = df.rename(columns={'batch_number': 'batch_id', 'sku_name': 'harvest_variant_name'})
    transformed_df['date'] = pd.to_datetime(transformed_df['date'], errors='coerce').dt.date
    transformed_df['loading_datetime'] = datetime.now(pytz.timezone('Asia/Jakarta')).replace(tzinfo=None)
    transformed_df.dropna(subset=['date', 'gh_name', 'harvest_variant_name', 'bruto_kg'], inplace=True)

    # PK Check - Remove duplicates based on primary key columns
    transformed_df = transformed_df[~transformed_df.duplicated(subset=['date', 'batch_id', 'gh_name', 'bruto_kg'], keep='first')]

    # Aggregation
    transformed_df = transformed_df.groupby(['date', 'batch_id', 'gh_name'], as_index=False).agg({
        'bruto_kg': 'sum',
        'harvest_variant_name': 'first',
        'loading_datetime': 'first'
    })

    # FK Check
    client = get_bigquery_client()
    existing_gh_codes = client.query(f"SELECT gh_code FROM `{LOOKUP_TABLE_ONE}`").to_dataframe()['gh_code']
    existing_varieties = client.query(f"SELECT harvest_variant_name FROM `{LOOKUP_TABLE_TWO}`").to_dataframe()['harvest_variant_name']
    existing_batches = client.query(f"SELECT batch_id FROM `{LOOKUP_TABLE_THREE}`").to_dataframe()['batch_id']

    valid_gh_codes = transformed_df['gh_name'].isin(existing_gh_codes)
    valid_varieties = transformed_df['harvest_variant_name'].isin(existing_varieties)
    valid_batch_master = transformed_df['batch_id'].isin(existing_batches)

    passed_df = transformed_df[valid_gh_codes & valid_varieties & valid_batch_master].reset_index(drop=True)
    error_df = transformed_df[~(valid_gh_codes & valid_varieties & valid_batch_master)].reset_index(drop=True)

    return passed_df, error_df

def load_data(df, target_table, **kwargs):
    if df.empty:
        logger.info("No data to load.")
        return
    
    client = get_bigquery_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        client.load_table_from_dataframe(df, target_table, job_config=job_config)
        logger.info(f"Loaded {df.shape[0]} records into {target_table}.")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")

def load_passed_data(**kwargs):
    ti = kwargs['ti']
    passed_df, _ = ti.xcom_pull(task_ids='transform_data')
    load_data(passed_df, TARGET_TABLE_PASSED, **kwargs)

def load_error_data(**kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')
    load_data(error_df, TARGET_TABLE_ERROR, **kwargs)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Jakarta')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'tc_2_daily_inbound_24',
    default_args=default_args,
    description='DAG for processing daily inbound data',
    schedule_interval=None,
    catchup=False,
    tags=['t2_daily_inbound_2024']
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
