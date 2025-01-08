from datetime import datetime, timedelta
import pytz
import pandas as pd
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from google.cloud import bigquery

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.batch_master'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.batch_master' # adjust to ckp_dds
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.batch_master_err'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'
LOOKUP_TABLE_ONE = 'biap-datainfra-gcp.ckp_dds.gh_master' # adjust to ckp_dds
LOOKUP_TABLE_TWO = 'biap-datainfra-gcp.global_dds.vegetable_master' # adjust to ckp_dds

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
            batch_id, 
            gh_name,  
            vegetable_variety,
            batch_start_date,
            transplant_date,
            batch_end_date,
            number_population
        FROM `{SOURCE_TABLE}`
    """
    
    df = client.query(query).to_dataframe()
    logger.info("Data extracted successfully.")
    logger.info(f"Extracted DataFrame shape: {df.shape}")
    return df

def add_null_flags(df, columns):
    """
    Function to create null check columns for multiple columns.
    """
    for col in columns:
        df[f'{col}_null_flag'] = df[col].isnull().astype(int)
    return df

def add_duplicate_flags(df, pk_columns):
    """
    Function to add a duplicate flag for rows based on primary key columns.
    """
    df['duplicate_flag'] = df.duplicated(subset=pk_columns, keep=False).astype(int)
    return df

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()
    transformed_df['number_population'] = transformed_df['number_population'].fillna(0).astype(int)

    # Rename the column to match the target table schema
    transformed_df.rename(columns={'number_population': 'original_population'}, inplace=True)

    # Convert date columns and handle defaults
    transformed_df['batch_start_date'] = pd.to_datetime(transformed_df['batch_start_date'], errors='coerce').dt.date
    transformed_df['transplant_date'] = pd.to_datetime(transformed_df['transplant_date'], errors='coerce').dt.date
    transformed_df['batch_end_date'] = pd.to_datetime(transformed_df['batch_end_date'], errors='coerce').dt.date.fillna(pd.Timestamp('2099-12-31').date())

    # Add loading_datetime column
    tz = pytz.timezone('Asia/Jakarta')
    transformed_df['loading_datetime'] = datetime.now(tz)

    # Initialize error dataframe
    error_df = pd.DataFrame()

    # -------- FK Check 1: gh_name --------
    # Extract LOOKUP_TABLE_ONE and rename 'gh_code' to 'gh_name'
    lookup_query_one = f"SELECT gh_code AS gh_name FROM `{LOOKUP_TABLE_ONE}`"
    lookup_df_one = client.query(lookup_query_one).to_dataframe()

    # FK Check: Left join SOURCE_TABLE (transformed_df) with LOOKUP_TABLE_ONE (lookup_df_one) on 'gh_name'
    merged_df_one = pd.merge(transformed_df, lookup_df_one, on='gh_name', how='left', indicator=True)
    
    # Mark rows where there is no match in LOOKUP_TABLE_ONE (i.e., NULL in 'gh_name' from lookup)
    merged_df_one['fk_check_gh_name'] = merged_df_one['_merge'].apply(lambda x: 1 if x == 'left_only' else 0)
    
    # Separate FK Check Failed rows (where 'fk_check_gh_name' == 1) into error_df
    error_df_one = merged_df_one[merged_df_one['fk_check_gh_name'] == 1].copy()
    if not error_df_one.empty:
        error_df_one['error_type'] = 'Foreign Key Check Failed: gh_name'
        error_df_one['error_description'] = 'No match found in LOOKUP_TABLE_ONE for gh_name.'
        error_df_one['flagging'] = 1
        error_df = pd.concat([error_df, error_df_one], ignore_index=True)
    
    # Filter out the rows where FK Check failed (keep only 'fk_check_gh_name' == 0)
    transformed_df = merged_df_one[merged_df_one['fk_check_gh_name'] == 0].drop(columns=['_merge', 'fk_check_gh_name'])

    # -------- FK Check 2: vegetable_variety --------
    # Extract LOOKUP_TABLE_TWO and rename 'vegetable_variant' to 'vegetable_variety'
    lookup_query_two = f"SELECT vegetable_variant AS vegetable_variety FROM `{LOOKUP_TABLE_TWO}`"
    lookup_df_two = client.query(lookup_query_two).to_dataframe()

    # FK Check: Left join SOURCE_TABLE (transformed_df) with LOOKUP_TABLE_TWO (lookup_df_two) on 'vegetable_variety'
    merged_df_two = pd.merge(transformed_df, lookup_df_two, on='vegetable_variety', how='left', indicator=True)
    
    # Mark rows where there is no match in LOOKUP_TABLE_TWO (i.e., NULL in 'vegetable_variety' from lookup)
    merged_df_two['fk_check_vegetable_variety'] = merged_df_two['_merge'].apply(lambda x: 1 if x == 'left_only' else 0)

    # Separate FK Check Failed rows (where 'fk_check_vegetable_variety' == 1) into error_df
    error_df_two = merged_df_two[merged_df_two['fk_check_vegetable_variety'] == 1].copy()
    if not error_df_two.empty:
        error_df_two['error_type'] = 'Foreign Key Check Failed: vegetable_variety'
        error_df_two['error_description'] = 'No match found in LOOKUP_TABLE_TWO for vegetable_variety.'
        error_df_two['flagging'] = 1
        error_df = pd.concat([error_df, error_df_two], ignore_index=True)

    # Filter out the rows where FK Check failed (keep only 'fk_check_vegetable_variety' == 0)
    transformed_df = merged_df_two[merged_df_two['fk_check_vegetable_variety'] == 0].drop(columns=['_merge', 'fk_check_vegetable_variety'])

    # Drop the flags (foreign key check columns) before loading data
    columns_to_drop = ['fk_check_gh_name', 'fk_check_vegetable_variety']
    transformed_df = transformed_df.drop(columns=[col for col in columns_to_drop if col in transformed_df.columns], errors='ignore')

    logger.info(f"Error DataFrame shape: {error_df.shape}")
    logger.info(f"Passed DataFrame shape: {transformed_df.shape}")

    # Return the passed_df and error_df
    return transformed_df, error_df

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, _ = ti.xcom_pull(task_ids='transform_data')
    
    client = get_bigquery_client(service_account_path)
    
    logger.info(f"Records to insert: {records_to_insert.shape[0]}")
    
    if not records_to_insert.empty:
        try:
            # Use WRITE_TRUNCATE to replace existing data in the target table
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(records_to_insert, TARGET_TABLE_PASSED, job_config=job_config)
            logger.info("Inserted new records successfully.")
        except Exception as e:
            logger.error(f"Failed to insert new records: {str(e)}")
    else:
        logger.warning("No records to insert.")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    logger.info(f"Loading Error DataFrame shape: {error_df.shape}")

    if not error_df.empty:
        try:
            table_id = TARGET_TABLE_ERROR
            # Use WRITE_TRUNCATE to replace existing data in the error table
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
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
    'tc_1_batch_master',
    default_args=default_args,
    description='DAG for moving data from one BigQuery dataset to another',
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['t1_ckp_batch_master_table']
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
