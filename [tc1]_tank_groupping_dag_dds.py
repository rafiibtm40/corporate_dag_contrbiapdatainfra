from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import pytz


# Constants
SOURCE_TABLE_HISTORY = 'biap-datainfra-gcp.ckp_stg.ckp_mitra_history'
SOURCE_TABLE_ALLOCATION = 'biap-datainfra-gcp.ckp_stg.pic_gh_allocation_history'
TARGET_TABLE = 'biap-datainfra-gcp.ckp_dds.tank_groupping' # Change to DDS
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.tank_groupping_error'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    client = get_bigquery_client(service_account_path)
    
    # Query for pic_gh_allocation table (with necessary columns)
    query_allocation = f"""
        SELECT 
            gh,
            pic_1,
            phase,
            coverage_size
        FROM `{SOURCE_TABLE_ALLOCATION}`
    """
    
    # Query for ckp_mitra_history table
    query_history = f"""
        SELECT 
            farmer_id,
            farmer_name,
            tank_assign,
            gh_name,
            status,
            ho_date,
            end_date
        FROM `{SOURCE_TABLE_HISTORY}`
    """
    
    try:
        # Extract the data from both tables
        df_allocation = client.query(query_allocation).to_dataframe()
        
        # Rename 'coverage_size' to 'area_sqm'
        df_allocation = df_allocation.rename(columns={'coverage_size': 'area_sqm'})
        
        df_history = client.query(query_history).to_dataframe()
        
        logger.info("Data extracted successfully.")
        logger.info(f"Extracted DataFrame from pic_gh_allocation: \n{df_allocation.head()}")
        logger.info(f"Extracted DataFrame from ckp_mitra_history: \n{df_history.head()}")
        
        return df_history, df_allocation
    except Exception as e:
        logger.error(f"Failed to extract data from BigQuery: {str(e)}")
        raise

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df_history, df_allocation = ti.xcom_pull(task_ids='extract_data')
    
    if df_history is None or df_history.empty or df_allocation is None or df_allocation.empty:
        logger.error("No data returned from extract_data. Aborting transform.")
        raise ValueError("No data returned from extract_data.")
    
    # Perform an inner join between df_allocation and df_history on 'gh_name'
    merged_df = pd.merge(
        df_allocation, 
        df_history[['gh_name', 'farmer_name', 'farmer_id', 'tank_assign', 'status', 'ho_date', 'end_date']], 
        left_on='gh', 
        right_on='gh_name', 
        how='left'
    )
    
    # Combine 'farmer_name' and 'pic_1' into a new column 'farmer_name_combined'
    merged_df['farmer_name_combined'] = merged_df['farmer_name'].fillna('') + merged_df['pic_1'].fillna('')
    
    # Check if 'tank_assign' and 'farmer_name_combined' are in the merged DataFrame
    if 'tank_assign' not in merged_df.columns or 'farmer_name_combined' not in merged_df.columns:
        logger.error(f"Missing required columns in merged DataFrame: {merged_df.columns.tolist()}")
        raise KeyError("Missing required columns: 'tank_assign' or 'farmer_name_combined'")

    # Check for records with missing required fields and handle errors
    error_df = merged_df[merged_df.isnull().any(axis=1)].reset_index(drop=True)
    
    # Drop rows with missing values in 'farmer_name_combined' and 'tank_assign' as these are required fields
    passed_df = merged_df.dropna(subset=['farmer_name_combined', 'tank_assign'], how='any').reset_index(drop=True)
    
    logger.info(f"Transformed DataFrame shape after dropping nulls: {passed_df.shape}")
    
    # Add loading_datetime column in GMT+7 (Asia/Bangkok timezone)
    tz = pytz.timezone('Asia/Bangkok')  # GMT+7
    passed_df['loading_datetime'] = datetime.now(tz)
    
    # Ensure 'loading_datetime' is in the correct datetime format
    passed_df['loading_datetime'] = pd.to_datetime(passed_df['loading_datetime'])
    
    # Select only the columns that match the BigQuery schema
    passed_df = passed_df[['gh_name', 'area_sqm', 'pic_1', 'farmer_id', 'farmer_name_combined', 'tank_assign', 'status', 'ho_date', 'end_date', 'loading_datetime']]
    
    # Rename columns to match target schema if necessary
    passed_df.rename(columns={
        'pic_1': 'pic_1',  # already correct
        'farmer_id': 'farmer_id',
        'farmer_name_combined': 'farmer_name_combined',  # renamed to combined farmer name
        'tank_assign': 'tank_assign',
        'status': 'status',
        'ho_date': 'ho_date',
        'end_date': 'end_date',
        'loading_datetime': 'loading_datetime',
        'gh_name': 'gh_name',
        'area_sqm': 'area_sqm',
    }, inplace=True)
    
    # Drop 'loading_datetime' from error_df as it's not necessary for the error table
    error_df = error_df[['gh_name', 'area_sqm', 'pic_1', 'farmer_id', 'farmer_name', 'tank_assign', 'status', 'ho_date', 'end_date']]
    
    return passed_df, error_df


def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, _ = ti.xcom_pull(task_ids='transform_data')
    
    if records_to_insert is None or records_to_insert.empty:
        logger.warning("No records to insert into passed table.")
        return

    client = get_bigquery_client(service_account_path)
    
    logger.info(f"Records to insert into {TARGET_TABLE}: {records_to_insert.shape[0]}")
    
    try:
        # Truncate the target table before loading new data
        truncate_query = f"TRUNCATE TABLE `{TARGET_TABLE}`"
        client.query(truncate_query).result()
        logger.info(f"Truncated table {TARGET_TABLE} successfully.")
        
        # Load the new records
        client.load_table_from_dataframe(records_to_insert, TARGET_TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
        logger.info("Inserted new records successfully.")
    except Exception as e:
        logger.error(f"Failed to insert new records into {TARGET_TABLE}: {str(e)}")
        raise

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    if error_df is None or error_df.empty:
        logger.info("No error data to load.")
        return

    client = get_bigquery_client(service_account_path)

    logger.info(f"Loading Error DataFrame shape before adjustment: {error_df.shape}")
    
    try:
        client.load_table_from_dataframe(error_df, TARGET_TABLE_ERROR, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
        logger.info("Error data loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load error data into {TARGET_TABLE_ERROR}: {str(e)}")
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
    't1_tank_groupping_dag',
    default_args=default_args,
    description='DAG for processing tank_groupping data',
    schedule_interval='0 18 * * *',  # Daily at 18:00
    catchup=False,
    tags=['t1_tank_groupping']
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
