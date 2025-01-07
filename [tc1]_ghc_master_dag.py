from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.gh_construction'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.gh_master'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.gh_construction_err'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Define the timezone
timezone = pytz.timezone('Asia/Bangkok')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': timezone.localize(datetime(2024, 1, 1)),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'tc_1_gh_construction_master_dag_dds',
    default_args=default_args,
    description='DAG for moving data from CKPs to DDS',
    schedule_interval='0 18 * * *',  # Adjust your schedule as needed
    catchup=False,
    tags=['t1_ckp_gh_master_table']
)

# Function to initialize BigQuery client
def get_bigquery_client():
    try:
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        logging.info("BigQuery client initialized successfully.")
        return client
    except Exception as e:
        logging.error(f"Error initializing BigQuery client: {e}")
        raise

# Function to extract data from BigQuery
def extract_data(query):
    client = get_bigquery_client()
    try:
        df = client.query(query).to_dataframe()
        logging.info("Data extracted successfully.")
        return df
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

# Task to extract data
def extract_task_func(**kwargs):
    query = f"""
    SELECT phase, phase_breakdown, site, nama_gh, type, area_sqm, gable, 
           table, holes_or_polybag, tandon, tandon_netsuite, gh_code, gh_code_lama, phase_complete, tank_assign
    FROM `{SOURCE_TABLE}`
    """
    df = extract_data(query)
    return df.to_json()

# Function to check for leading, trailing, and double spaces
def check_spaces(df, column_name):
    leading_spaces = df[column_name].str.startswith(' ')
    trailing_spaces = df[column_name].str.endswith(' ')
    double_spaces = df[column_name].str.contains(r'\s{2,}', na=False)
    return leading_spaces, trailing_spaces, double_spaces

# Task to transform data
def transform_task_func(**kwargs):
    df_json = kwargs['ti'].xcom_pull(task_ids='extract_data')
    df_gm = pd.read_json(df_json)

    # Rename columns according to the mappings
    df_gm.rename(columns={
        'nama_gh': 'gh_long_name',
        'type': 'construction_type',
        'gable': 'no_of_gables',
        'table': 'no_of_tables',
        'holes_or_polybag': 'no_of_polybags',
        'tandon': 'tandon_name',
        'tandon_netsuite': 'tandon_netsuite',
        'tank_assign':'tank_assign'
    }, inplace=True)

    # Convert 'no_of_tables' from string to integer, handling NaN values
    if 'no_of_tables' in df_gm:
        df_gm['no_of_tables'] = pd.to_numeric(df_gm['no_of_tables'], errors='coerce').fillna(0).astype(int)
        logging.info("Converted 'no_of_tables' to integers with NaN values handled.")
    else:
        logging.warning("'no_of_tables' column is missing after renaming.")

    # Initialize new columns
    df_gm['gh_name_count'] = df_gm.groupby('gh_code')['gh_code'].transform('count')
    df_gm['gh_name_na'] = df_gm['gh_code'].isnull().astype(int)

    
    # Set 'loading_datetime' column to current time (without timezone)
    df_gm['loading_datetime'] = datetime.now(timezone).replace(tzinfo=None)

    # Flagging logic
    conditions = [
        (df_gm['gh_name_count'] > 1) & (df_gm['gh_name_na'] == 0),
        (df_gm['gh_name_count'] == 1) & (df_gm['gh_name_na'] == 1),
    ]
    values = [1, 1]
    df_gm['flagging'] = np.select(conditions, values, default=0)

    # Split into passed and error DataFrames
    passed_df = df_gm[df_gm['flagging'] == 0]
    error_df = df_gm[df_gm['flagging'] > 0]

    # Check for spaces in the 'gh_long_name' column
    leading_spaces, trailing_spaces, double_spaces = check_spaces(df_gm, 'gh_code')
    error_df = pd.concat([error_df, df_gm[trailing_spaces | leading_spaces | double_spaces]])

    logging.info(f"Transform complete: {len(passed_df)} passed, {len(error_df)} errors.")
    
    # Push DataFrames to XCom
    return {
        'passed_df': passed_df.to_json(),
        'error_df': error_df.to_json()
    }

# Task to validate data
def validation_task_func(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(task_ids='transform_data')
    passed_df = pd.read_json(df_dict['passed_df'])
    error_df = pd.read_json(df_dict['error_df'])

    logging.info(f"Validation complete: {len(passed_df)} passed, {len(error_df)} errors.")

    return {
        'passed_df': passed_df.to_json(),
        'error_df': error_df.to_json()
    }

# Task to deliver passed DataFrame to BigQuery
def deliver_passed_data(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(task_ids='validate_data')
    passed_df = pd.read_json(df_dict['passed_df'])

    logging.info(f"Passed DataFrame before processing: {passed_df.head()}")

    if not passed_df.empty:
        # Ensure specific columns are treated as float
        float_columns = ['area_sqm', 'no_of_gables', 'no_of_tables']
        for col in float_columns:
            if col in passed_df:
                passed_df[col] = pd.to_numeric(passed_df[col], errors='coerce').fillna(0).astype(float)

        # Ensure other necessary columns are filled appropriately
        if 'gh_long_name' in passed_df:
            passed_df['gh_long_name'] = passed_df['gh_long_name'].fillna('').astype(str)

        # Ensure phase_breakdown exists
        if 'phase_breakdown' not in passed_df:
            logging.warning("Phase breakdown is missing from passed data.")
            passed_df['phase_breakdown'] = ''  # Fill with empty string or appropriate default value

        # Reorder columns to match BigQuery schema
        passed_df = passed_df[[ 
            'phase', 'gh_long_name', 'construction_type', 
            'area_sqm', 'no_of_gables', 'no_of_tables', 'no_of_polybags', 
            'tandon_name', 'tandon_netsuite', 'loading_datetime', 
            'gh_code', 'gh_code_lama','phase_breakdown', 'phase_complete','tank_assign'
        ]]

        client = get_bigquery_client()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", 
        )
        job = client.load_table_from_dataframe(
            passed_df, 
            TARGET_TABLE_PASSED,
            job_config=job_config
        )
        job.result()
        logging.info("Passed data loaded successfully.")
    else:
        logging.warning("Passed DataFrame is empty. No data to deliver.")

# Task to deliver error DataFrame to BigQuery
def deliver_error_data(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(task_ids='validate_data')
    error_df = pd.read_json(df_dict['error_df'])

    if not error_df.empty:
        # Ensure specific columns are treated as float
        float_columns = ['area_sqm', 'no_of_gables', 'no_of_tables']
        for col in float_columns:
            if col in error_df:
                error_df[col] = pd.to_numeric(error_df[col], errors='coerce').fillna(0).astype(float)

        # Ensure other necessary columns are filled appropriately
        if 'gh_long_name' in error_df:
            error_df['gh_long_name'] = error_df['gh_long_name'].fillna('').astype(str)

        # Ensure phase_breakdown exists
        if 'phase_breakdown' not in error_df:
            logging.warning("Phase breakdown is missing from error data.")
            error_df['phase_breakdown'] = ''  # Fill with empty string or appropriate default value

        # Reorder columns to match BigQuery schema
        error_df = error_df[[ 
            'phase', 'phase_breakdown', 'gh_long_name', 'construction_type', 
            'area_sqm', 'no_of_gables', 'no_of_tables', 'no_of_polybags', 
            'tandon_name', 'tandon_netsuite', 'gh_name_na', 
            'gh_name_count', 'loading_datetime', 'flagging', 
            'gh_code', 'gh_code_lama', 'phase_complete','tank_assign'
        ]]

        client = get_bigquery_client()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # This will append to the table
        )
        job = client.load_table_from_dataframe(
            error_df, 
            TARGET_TABLE_ERROR,
            job_config=job_config
        )
        job.result()
        logging.info("Error data loaded successfully.")
    else:
        logging.warning("Error DataFrame is empty. No data to deliver.")

# Define tasks in the DAG
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task_func,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task_func,
    provide_context=True,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validation_task_func,
    provide_context=True,
    dag=dag,
)

deliver_passed_data_task = PythonOperator(
    task_id='deliver_passed_data',
    python_callable=deliver_passed_data,
    provide_context=True,
    dag=dag,
)

deliver_error_data_task = PythonOperator(
    task_id='deliver_error_data',
    python_callable=deliver_error_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_data_task >> transform_data_task >> validate_data_task >> [deliver_passed_data_task, deliver_error_data_task]
