import pandas as pd
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from google.oauth2 import service_account
from datetime import datetime
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.batamindo_stg_dev.ckp_gh_status'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.gh_status'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.batamindo_stg_dev.ckp_gh_status_err'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'
LOOKUP_TABLE = 'biap-datainfra-gcp.ckp_dds.batch_master'

def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    client = get_bigquery_client(service_account_path)
    query = f"""
        SELECT 
            batch_id, 
            gh_name, 
            status, 
            start_date, 
            end_date,
            vegetable_variety,
            leader,
            pic,
            pic_2,
            actual_harvest,
            reason_to_exterminate,
            remarks,
            actual_population
        FROM `{SOURCE_TABLE}`"""
    
    df = client.query(query).to_dataframe()
    logging.info("Data extracted successfully.")
    logging.info(f"Extracted DataFrame shape: {df.shape}")
    return df

def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df = ti.xcom_pull(task_ids='extract_data')

    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()
    
    # Fill None values in 'actual_population' with a default value (e.g., 0)
    transformed_df['actual_population'] = transformed_df['actual_population'].fillna(0).astype(int)

    # Drop the 'actual_harvest' column
    transformed_df.drop(columns=['actual_harvest'], errors='ignore', inplace=True)

    # Convert date columns
    transformed_df['start_date'] = pd.to_datetime(transformed_df['start_date'], errors='coerce').dt.date
    transformed_df['end_date'] = pd.to_datetime(transformed_df['end_date'], errors='coerce').dt.date.fillna(pd.Timestamp('2099-12-31').date())
    
    # Convert 'pic' and 'pic_2' columns to string and replace NaNs with 'N/A'
    transformed_df['pic'] = transformed_df['pic'].astype(str).fillna('N/A')
    transformed_df['pic_2'] = transformed_df['pic_2'].astype(str).fillna('N/A')

    # Add loading_datetime column in the required format (GMT +7)
    tz = pytz.timezone('Asia/Jakarta')  # GMT +7
    transformed_df['loading_datetime'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    # Group by to check for duplicates
    grouped = transformed_df.groupby(['batch_id', 'gh_name', 'vegetable_variety', 'start_date']).size().reset_index(name='count')

    # Identify duplicates
    duplicates = grouped[grouped['count'] > 1]

    # Create error_df and passed_df based on duplicates
    if not duplicates.empty:
        error_df = transformed_df[transformed_df[['batch_id', 'gh_name', 'vegetable_variety', 'start_date']].apply(tuple, axis=1).isin(duplicates[['batch_id', 'gh_name', 'vegetable_variety', 'start_date']].apply(tuple, axis=1))]
        passed_df = transformed_df[~transformed_df[['batch_id', 'gh_name', 'vegetable_variety', 'start_date']].apply(tuple, axis=1).isin(duplicates[['batch_id', 'gh_name', 'vegetable_variety', 'start_date']].apply(tuple, axis=1))]
    else:
        passed_df = transformed_df
        error_df = pd.DataFrame(columns=transformed_df.columns)  # Create an empty DataFrame for errors if no duplicates found

    # FK Check
    existing_batches = client.query(f"SELECT batch_id FROM `{LOOKUP_TABLE}`").to_dataframe()
    existing_batches.reset_index(drop=True, inplace=True)  # Reset index to ensure alignment
    valid_batch_ids = passed_df['batch_id'].isin(existing_batches['batch_id'])

    # Apply valid_batch_ids using the correct indexing
    passed_df = passed_df[valid_batch_ids].reset_index(drop=True)

    if passed_df.shape[0] < valid_batch_ids.sum():  # Check against the count of valid_batch_ids
        invalid_batch_df = transformed_df[~valid_batch_ids].reset_index(drop=True)
        error_df = pd.concat([error_df, invalid_batch_df]).drop_duplicates()

    # Log the DataFrame shapes
    logging.info(f"Records to insert: {passed_df.shape}")
    logging.info(f"Records to update: {passed_df.shape}")
    return passed_df, error_df

def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    # If there are records to insert
    if not records_to_insert.empty:
        try:
            # Insert new records into the target table
            client.load_table_from_dataframe(records_to_insert, TARGET_TABLE_PASSED, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
            logging.info("Inserted new records successfully.")
        except Exception as e:
            logging.error(f"Failed to insert new records: {str(e)}")
    
    # Upsert logic - MERGE
    if not records_to_insert.empty:
        merge_query = f"""
        MERGE INTO `{TARGET_TABLE_PASSED}` AS target
        USING (SELECT * FROM UNNEST({records_to_insert.to_dict(orient='records')})) AS source
        ON target.batch_id = source.batch_id
           AND target.gh_name = source.gh_name
           AND target.start_date = source.start_date
           AND target.vegetable_variety = source.vegetable_variety
        WHEN MATCHED AND (target.status != source.status OR 
                           target.leader != source.leader OR
                           target.pic != source.pic OR
                           target.pic_2 != source.pic_2 OR
                           target.actual_population != source.actual_population OR
                           target.reason_to_exterminate != source.reason_to_exterminate OR
                           target.remarks != source.remarks OR
                           target.end_date != source.end_date)
        THEN
            UPDATE SET
                target.status = source.status,
                target.leader = source.leader,
                target.pic = source.pic,
                target.pic_2 = source.pic_2,
                target.actual_population = source.actual_population,
                target.reason_to_exterminate = source.reason_to_exterminate,
                target.remarks = source.remarks,
                target.end_date = source.end_date,
                target.loading_datetime = source.loading_datetime
        WHEN NOT MATCHED THEN
            INSERT (batch_id, gh_name, status, start_date, end_date, vegetable_variety, leader, pic, pic_2, actual_harvest, reason_to_exterminate, remarks, actual_population, loading_datetime)
            VALUES (source.batch_id, source.gh_name, source.status, source.start_date, source.end_date, source.vegetable_variety, source.leader, source.pic, source.pic_2, source.actual_harvest, source.reason_to_exterminate, source.remarks, source.actual_population, source.loading_datetime)
        """
        
        try:
            client.query(merge_query)
            logging.info("Upsert operation (merge) completed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute merge query: {str(e)}")

def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    logging.info(f"Loading Error DataFrame shape: {error_df.shape}")

    if not error_df.empty:
        try:
            error_df.to_gbq(destination_table=TARGET_TABLE_ERROR, project_id=client.project, if_exists='append', credentials=client._credentials)
            logging.info("Error data loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to load error data: {str(e)}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
}

with DAG('t1_upsert_gh_status_dds',
         default_args=default_args,
         schedule_interval='0 18 * * *',
         catchup=False,
         tags=['t1_upsert_gh_status']
         ) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_passed_data_task = PythonOperator(
        task_id='load_passed_data',
        python_callable=load_passed_data,
        provide_context=True,
    )

    load_error_data_task = PythonOperator(
        task_id='load_error_data',
        python_callable=load_error_data,
        provide_context=True,
    )

    # Set task dependencies
    extract_data_task >> transform_data_task >> [load_passed_data_task, load_error_data_task]
