from datetime import datetime
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from airflow.utils.dates import days_ago
import pendulum  # Use pendulum for timezone handling

# Define the path to the service account key
SERVICE_ACCOUNT_KEY_PATH = '/home/corporate/myKeys/airflowbiapvm.json'

# Function to initialize BigQuery client
def get_bigquery_client():
    return bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_PATH)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Adjust the start date accordingly
    'retries': 1,
    'timezone': 'UTC'  # Set timezone here instead of in the DAG constructor
}

# Initialize the Master DAG
master_dag = DAG(
    't1_trigger_ckp_master_dag',
    default_args=default_args,
    description='Master DAG to orchestrate other DAGs',
    schedule_interval='0 18 * * *',  # Triggered once a day
    start_date=datetime(2023, 10, 1),  # or use days_ago() for dynamic start date
    catchup=False,
    tags=['data_pipeline'],
    # Removed timezone parameter here
)

# Function to check data availability for all master tables
def check_data_availability():
    client = get_bigquery_client()
    master_tables = [
        'biap-datainfra-gcp.batamindo_stg.daily_inbound_2024_ckp', # ini dari batamindo_stg
        'biap-datainfra-gcp.global_stg.vegetable_master',
        'biap-datainfra-gcp.ckp_stg.gh_construction',
        'biap-datainfra-gcp.global_stg.harvest_master',
        'biap-datainfra-gcp.ckp_stg.batch_master',
        'biap-datainfra-gcp.ckp_stg.gh_status',
        'biap-datainfra-gcp.batamindo_stg_dev.ckp_mitra_group_database',
        'biap-datainfra-gcp.batamindo_stg_dev.pic_gh_allocation_history' # gh_status_stg
    ]
    
    for table in master_tables:
        query = f"SELECT COUNT(*) as record_count FROM `{table}`"
        result = client.query(query).result()
        for row in result:
            print(f"Record Count for {table}: {row.record_count}")

# Task to trigger Vegetable Master DAG
trigger_vegetable = TriggerDagRunOperator(
    task_id='trigger_vegetable_master',
    trigger_dag_id='t1_vegetable_master_dag_dds',  # Updated DAG ID
    dag=master_dag,
)

# Task to trigger Harvest Master DAG
trigger_harvest_master = TriggerDagRunOperator(
    task_id='trigger_harvest_master',
    trigger_dag_id='tc1_harvest_master_dag_dds',  # Assuming this is correct
    dag=master_dag,
)

# Task to trigger GH Master DAG (GH Construction)
trigger_gh_master = TriggerDagRunOperator(
    task_id='trigger_gh_construction_master',
    trigger_dag_id='tc_1_gh_construction_master_dag_dds',  # Updated DAG ID
    dag=master_dag,
)

# Task to trigger Batch Master DAG
trigger_batch_master = TriggerDagRunOperator(
    task_id='trigger_batch_master',
    trigger_dag_id='tc_1_batch_master',  # Updated DAG ID
    dag=master_dag,
)

# Task to trigger Tank Grouping DAG
trigger_tank_groupping = TriggerDagRunOperator(
    task_id='trigger_tank_groupping',
    trigger_dag_id='t1_tank_groupping_dag',  # Updated DAG ID
    dag=master_dag,
)

# Task to trigger Mitra Grouping DAG
trigger_mitra_group_database = TriggerDagRunOperator(
    task_id='trigger_mitra_group_database',
    trigger_dag_id='mitra_group_database_dag',  # Updated DAG ID
    dag=master_dag,
)

# Task to trigger GH Status DAG
trigger_gh_status = TriggerDagRunOperator(
    task_id='trigger_gh_status',
    trigger_dag_id='t1_upsert_gh_status_dds',  # Assuming this is correct
    dag=master_dag,
)

# Task to trigger Harvest 2024 DAG
trigger_harvest = TriggerDagRunOperator(
    task_id='trigger_harvest',
    trigger_dag_id='tc_1_daily_inbound_24_insert_update_revise',  # Assuming this is correct
    dag=master_dag,
)

# Example task to check data availability (optional)
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=master_dag,
)

# Set task dependencies to follow the sequence:

# Start the sequence from vegetable master
trigger_vegetable >> trigger_harvest_master >> trigger_gh_master >> trigger_batch_master >> trigger_tank_groupping >> trigger_mitra_group_database >> trigger_gh_status >> trigger_harvest
