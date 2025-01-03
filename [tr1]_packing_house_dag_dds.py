from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import logging
import pytz

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_stg.packing_house'
TARGET_TABLE_PASSED = 'biap-datainfra-gcp.ckp_dds.packing_house'
TARGET_TABLE_ERROR = 'biap-datainfra-gcp.ckp_stg.packing_house_err'
JOINED_TABLE_ONE = 'biap-datainfra-gcp.global_dds.harvest_master'
JOINED_TABLE_TWO = 'biap-datainfra-gcp.ckp_dds.gh_master'
SERVICE_ACCOUNT_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

# BigQuery client setup
def get_bigquery_client(service_account_path=SERVICE_ACCOUNT_PATH):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials)

# Extract data from BigQuery
def extract_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    client = get_bigquery_client(service_account_path)
    query = f"""
        SELECT 
            tanggal_proses,
            tanggal_panen,
            gh_actual,
            harvest_variant_name,
            harvest_name_final,
            bruto_kg,
            netto_kg,
            waste_tangkai,
            waste_buah_busuk
        FROM `{SOURCE_TABLE}`
    """
    
    df = client.query(query).to_dataframe()
    logger.info("Data extracted successfully.")
    logger.info(f"Extracted DataFrame shape: {df.shape}")
    return df

# Transform data (validation and cleaning)
def transform_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    client = get_bigquery_client(service_account_path)
    
    df = ti.xcom_pull(task_ids='extract_data')
    
    if df is None or df.empty:
        raise ValueError("No data returned from extract_data.")
    
    transformed_df = df.copy()

    # Rename the columns to match the target table schema
    transformed_df['bruto_kg'] = df['bruto_kg']

    # Add loading_datetime column in GMT+7
    tz = pytz.timezone('Asia/Bangkok')  # GMT+7
    transformed_df['loading_datetime'] = datetime.now(tz)  # Set as DATETIME

    # Retrieve the data from FK table
    query = f"""
    SELECT 
        new_code,
        harvest_name 
    FROM {JOINED_TABLE_ONE}
    """
    hm_df = client.query(query).to_dataframe()
    logger.info("harvest_master_table data retrieved successfully.")

    # Retrieve the data from another FK table
    query = f"""
    SELECT 
        gh_code,
        phase_breakdown 
    FROM {JOINED_TABLE_TWO}
    """
    gm_df = client.query(query).to_dataframe()
    logger.info("gh_master data retrieved successfully.")

    # Step 1: Data validation (Null checks)
    error_df = transformed_df[transformed_df[['tanggal_proses', 'tanggal_panen', 'gh_actual', 'bruto_kg']].isnull().any(axis=1)]
    logger.info(f"Error DataFrame shape after null check: {error_df.shape}")

    # Step 2: Check for duplicates on primary key fields
    duplicate_flag = transformed_df.duplicated(subset=['tanggal_proses', 'gh_actual','tanggal_panen','bruto_kg','netto_kg'], keep=False).astype(int)
    transformed_df['duplicate_flag'] = duplicate_flag
    error_df = pd.concat([error_df, transformed_df[transformed_df['duplicate_flag'] == 1]])

    logger.info(f"Error DataFrame shape after duplicate check: {error_df.shape}")
    
    # FK Check for 'harvest_name_final' in 'harvest_master'
    fk_harvest_name_check = transformed_df[~transformed_df['harvest_name_final'].isin(hm_df['harvest_name'])]
    error_df = pd.concat([error_df, fk_harvest_name_check])
    
    # FK Check for 'gh_master_final' in 'gh_master'
    fk_gh_actual_check = transformed_df[~transformed_df['gh_actual'].isin(gm_df['gh_code'])]
    error_df = pd.concat([error_df, fk_gh_actual_check])
    
    logger.info(f"Error DataFrame shape after FK checks: {error_df.shape}")

    # Step 3: Separate passed and error DataFrames
    passed_df = transformed_df[transformed_df['duplicate_flag'] == 0].reset_index(drop=True)
    error_df = error_df.reset_index(drop=True)

    # Clean up the columns to match the target schema
    target_columns = ['tanggal_proses', 'tanggal_panen', 'gh_actual', 'harvest_variant_name', 'harvest_name_final', 'bruto_kg', 'netto_kg', 'waste_tangkai', 'waste_buah_busuk', 'loading_datetime']
    passed_df = passed_df[target_columns]

    # Add error-specific columns for the error table
    error_df['error_type'] = 'PK_CHECK_FAILURE'
    error_df['error_description'] = 'Duplicate primary key or missing required field(s).'
    error_df['flagging'] = 1
    error_df['tanggal_panen_na'] = error_df['tanggal_panen'].isnull().astype(str)
    error_df['harvest_name_final_na'] = error_df['harvest_name_final'].isnull().astype(str)
    error_df['bruto_kg_na'] = error_df['bruto_kg'].isnull().astype(float)
    error_df['netto_kg_na'] = error_df['netto_kg'].isnull().astype(float)
    error_df['waste_tangkai_na'] = error_df['waste_tangkai'].isnull().astype(float)
    error_df['waste_buah_busuk_na'] = error_df['waste_buah_busuk'].isnull().astype(float)
    error_df['duplicate_flag'] = transformed_df['duplicate_flag'].astype(int)
    error_df['harvest_name_count'] = transformed_df.groupby('tanggal_proses')['tanggal_panen'].transform('count')

    logger.info(f"Passed DataFrame shape after cleanup: {passed_df.shape}")
    logger.info(f"Error DataFrame shape after cleanup: {error_df.shape}")

    return passed_df, error_df

# Load passed data to BigQuery with UPSERT logic
def load_passed_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    records_to_insert, _ = ti.xcom_pull(task_ids='transform_data')
    
    client = get_bigquery_client(service_account_path)
    
    logger.info(f"Records to insert: {records_to_insert.shape[0]}")

    if not records_to_insert.empty:
        # Prepare a list for insert and update
        insert_data = []
        update_data = []
        
        for _, row in records_to_insert.iterrows():
            # Build the WHERE condition based on the primary key
            where_condition = f"""
                tanggal_proses = '{row['tanggal_proses']}'
                AND tanggal_panen = '{row['tanggal_panen']}'
                AND gh_actual = '{row['gh_actual']}'
                AND bruto_kg = {row['bruto_kg']}
                AND harvest_name_final = '{row['harvest_name_final']}'
            """
            
            # Query the target table to see if the record already exists
            query = f"""
                SELECT COUNT(*) as count
                FROM `{TARGET_TABLE_PASSED}`
                WHERE {where_condition}
            """
            existing_record = client.query(query).to_dataframe()
            record_exists = existing_record['count'][0] > 0

            # If the record exists, add it to update data
            if record_exists:
                update_data.append(row)
            else:
                insert_data.append(row)

        # If there is data to insert
        if insert_data:
            insert_df = pd.DataFrame(insert_data)
            try:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")  # Use append for insert
                client.load_table_from_dataframe(insert_df, TARGET_TABLE_PASSED, job_config=job_config)
                logger.info(f"Inserted {len(insert_df)} records successfully.")
            except Exception as e:
                logger.error(f"Failed to insert new records: {str(e)}")

        # If there is data to update
        if update_data:
            # Update each record individually
            for row in update_data:
                # Define an UPDATE query for each record
                update_query = f"""
                    UPDATE `{TARGET_TABLE_PASSED}`
                    SET
                        netto_kg = {row['netto_kg']},
                        waste_tangkai = {row['waste_tangkai']},
                        waste_buah_busuk = {row['waste_buah_busuk']},
                        loading_datetime = '{row['loading_datetime'].strftime('%Y-%m-%d %H:%M:%S')}'
                    WHERE
                        tanggal_proses = '{row['tanggal_proses']}'
                        AND tanggal_panen = '{row['tanggal_panen']}'
                        AND gh_actual = '{row['gh_actual']}'
                        AND bruto_kg = {row['bruto_kg']}
                        AND harvest_name_final = '{row['harvest_name_final']}'
                """
                try:
                    client.query(update_query)
                    logger.info(f"Updated record for {row['tanggal_proses']} - {row['tanggal_panen']}.")
                except Exception as e:
                    logger.error(f"Failed to update record for {row['tanggal_proses']} - {row['tanggal_panen']}: {str(e)}")

    else:
        logger.warning("No records to process.")


# Load error data to BigQuery
def load_error_data(service_account_path=SERVICE_ACCOUNT_PATH, **kwargs):
    ti = kwargs['ti']
    _, error_df = ti.xcom_pull(task_ids='transform_data')

    client = get_bigquery_client(service_account_path)

    logger.info(f"Loading Error DataFrame shape before adjustment: {error_df.shape}")

    if not error_df.empty:
        try:
            table_id = TARGET_TABLE_ERROR
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
    'start_date': datetime(2023, 10, 1, tzinfo=pytz.timezone('Asia/Bangkok')),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
}

# Initialize the DAG
dag = DAG(
    'packing_house_dag_dds',
    default_args=default_args,
    description='DAG for processing packing_house',
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['dev_ckp_master_table']
)

# Task definitions
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_passed_data_task = PythonOperator(
    task_id='load_passed_data',
    python_callable=load_passed_data,
    execution_timeout=timedelta(minutes=60),
    dag=dag
)

load_error_data_task = PythonOperator(
    task_id='load_error_data',
    python_callable=load_error_data,
    dag=dag
)

# Set task dependencies
extract_data_task >> transform_data_task >> [load_passed_data_task, load_error_data_task]
