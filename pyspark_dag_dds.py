from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import pandas as pd
import logging
import uuid
from cleanup import cleanup_xcom

# Configure logging
logging.basicConfig(level=logging.INFO)

# Path to the service account JSON
SERVICE_ACCOUNT_JSON_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),  # Adjust the start date accordingly
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'spark_daily_inbound_2024_dag',
    default_args=default_args,
    description='DAG for processing daily inbound data for 2024',
    schedule_interval='@daily',  # Daily schedule
)

# Function to initialize BigQuery client
def get_bigquery_client():
    return bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON_PATH)

# Function to extract data from BigQuery
def extract_data(query):
    try:
        client = get_bigquery_client()
        df = client.query(query).to_dataframe()
        logging.info("Data extracted successfully.")
        return df
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

# Task to extract data
def extract_task_func(**kwargs):
    query = """
    SELECT * FROM biap-datainfra-gcp.batamindo_stg_dev.ckp_daily_inbound_2024
    """
    try:
        df = extract_data(query)
        return df.to_json()
    except Exception as e:
        logging.error(f"Error in extract_task_func: {e}")
        raise

# Task to transform and validate data
def transform_task_func(**kwargs):
    try:
        df_json = kwargs['ti'].xcom_pull(task_ids='extract_data')
        df_di = pd.read_json(df_json)

        # Foreign Key Checks
        valid_batches = extract_data("SELECT batch_number FROM biap-datainfra-gcp.batamindo_stg_dev.ckp_batch_number")['batch_number'].unique()
        valid_gh_names = extract_data("SELECT nama_gh FROM biap-datainfra-gcp.batamindo_stg_dev.ckp_gh_construction")['nama_gh'].unique()
        valid_sku_names = extract_data("SELECT sku_variant_name FROM biap-datainfra-gcp.batamindo_stg_dev.ckp_harvest_master")['sku_variant_name'].unique()

        errors = []
        
        if not df_di['batch_number'].isin(valid_batches).all():
            errors.append("Invalid batch_number found.")

        if not df_di['gh_name'].isin(valid_gh_names).all():
            errors.append("Invalid gh_name found.")

        if not df_di['sku_name'].isin(valid_sku_names).all():
            errors.append("Invalid sku_name found.")

        error_df = pd.DataFrame()
        if errors:
            error_df = df_di[
                ~df_di['batch_number'].isin(valid_batches) |
                ~df_di['gh_name'].isin(valid_gh_names) |
                ~df_di['sku_name'].isin(valid_sku_names)
            ]

        df_di['surrogate_key'] = (
            df_di['sku_name'] + "_" + 
            df_di['gh_name'] + "_" + 
            df_di['batch_number'] + "_" + 
            df_di['date'].astype(str)
        )

        duplicate_keys = df_di['surrogate_key'].duplicated(keep=False)
        null_keys = df_di['surrogate_key'].isnull()

        if duplicate_keys.any() or null_keys.any():
            error_df = pd.concat([error_df, df_di[duplicate_keys | null_keys]])

        passed_df = df_di[~df_di.index.isin(error_df.index)]

        return {
            'passed_df': passed_df.to_json(),
            'error_df': error_df.to_json()
        }
    except Exception as e:
        logging.error(f"Error in transform_task_func: {e}")
        raise

# Task to deliver passed DataFrame to BigQuery
def deliver_passed_data(**kwargs):
    try:
        df_dict = kwargs['ti'].xcom_pull(task_ids='transform_data')
        passed_df = pd.read_json(df_dict['passed_df'])

        if not passed_df.empty:
            client = get_bigquery_client()
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
            job = client.load_table_from_dataframe(passed_df, 'biap-datainfra-gcp.batamindo_dds.daily_inbound_2024_ckp_dds', job_config=job_config)
            job.result()
            logging.info("Passed data delivered to BigQuery.")
    except Exception as e:
        logging.error(f"Error delivering passed data: {e}")
        raise

# Task to deliver error DataFrame to BigQuery
def deliver_error_data(**kwargs):
    try:
        df_dict = kwargs['ti'].xcom_pull(task_ids='transform_data')
        error_df = pd.read_json(df_dict['error_df'])

        if not error_df.empty:
            client = get_bigquery_client()
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
            job = client.load_table_from_dataframe(error_df, 'biap-datainfra-gcp.batamindo_stg_dev.ckp_daily_inbound_2024_err', job_config=job_config)
            job.result()
            logging.info("Error data delivered to BigQuery.")
    except Exception as e:
        logging.error(f"Error delivering error data: {e}")
        raise

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
extract_data_task >> transform_data_task >> [deliver_passed_data_task, deliver_error_data_task]
