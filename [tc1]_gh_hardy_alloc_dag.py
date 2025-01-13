from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator
import numpy as np

# Constants
SOURCE_TABLE = 'biap-datainfra-gcp.ckp_dds.gh_status' # ckp_dds
JOINED_TABLE_ONE = 'biap-datainfra-gcp.ckp_dds.harvest'# ckp_dds
JOINED_TABLE_TWO = 'biap-datainfra-gcp.ckp_dds.gh_master' # ckp_dds
JOINED_TABLE_THREE = 'biap-datainfra-gcp.ckp_dds.batch_master' # ckp_dds
JOINED_TABLE_FOUR = 'biap-datainfra-gcp.global_dds.harvest_master' # global_dds
JOINED_TABLE_FIVE = 'biap-datainfra-gcp.global_dds.sakata_hst_ideal_yield' 
JOINED_TABLE_SIX = 'biap-datainfra-gcp.ckp_dds.tank_groupping_test' # ckp_dds
JOINED_TABLE_SEVEN = 'biap-datainfra-gcp.global_dds.vegetable_master' # global_dds
TARGET_TABLE = 'biap-datainfra-gcp.batamindo_ckp_dvm.raw_gh_status_for_gh_hardy_allocation' #batamindo ckp_dvm
SERVICE_ACCOUNT_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/home/corporate/myKeys/airflowbiapvm.json')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('airflow.task')

def get_bigquery_client():
    """Create a BigQuery client using the service account credentials."""
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
    return bigquery.Client(credentials=credentials)

def extract_data(**kwargs):
    """Extract data from BigQuery and return the most recent records."""
    client = get_bigquery_client()

    query = f"""
    WITH filtered_data AS (
        SELECT 
            batch_id
            , gh_name
            , status
            , start_date
            , end_date
            , vegetable_variety
            , reason_to_exterminate
            , actual_population
            , loading_datetime
            , ROW_NUMBER() OVER (PARTITION BY batch_id, gh_name ORDER BY start_date DESC) AS row_num
        FROM `{SOURCE_TABLE}`
    )
    SELECT
        batch_id
        , gh_name
        , status
        , start_date
        , end_date
        , vegetable_variety
        , actual_population
        , loading_datetime
    FROM filtered_data
    WHERE row_num >= 1 AND end_date = '2099-12-31'
    ORDER BY gh_name ASC
    """
    df = client.query(query).to_dataframe()

    # Log extracted data
    logger.info(f"Extracted DataFrame shape: {df.shape}")
    if df.empty:
        logger.warning("No data extracted! Ensure the query is correct.")

    # Sort data and handle 'loading_datetime'
    df['loading_datetime'] = pd.to_datetime(df['loading_datetime'])
    df = df.sort_values('loading_datetime', ascending=False)

    kwargs['ti'].xcom_push(key='gh_status_df', value=df)
    return df

def transform_data_one(**kwargs):  # join gh_status with jb_df
    """Perform transformation on the extracted data to match target schema."""
    client = get_bigquery_client()

    # Retrieve the extracted data from XCom
    gh_status_df = kwargs['ti'].xcom_pull(task_ids='extract_data', key='gh_status_df')

    # Step 2: Join batch_master and harvest within a query and name it 'jb_df'
    join_in_between_query = f"""
    SELECT
        d1.batch_id,
        d1.gh_name,
        d1.vegetable_variety,
        d1.batch_start_date,
        d1.transplant_date,
        d1.batch_end_date,
        d1.original_population,
        d1.vegetable_subcategory,
        SUM(df_inb.bruto_kg) AS bruto_kg
    FROM (
        SELECT
            df_bm.batch_id,
            df_bm.gh_name,
            df_bm.vegetable_variety AS vv_bm,
            df_bm.batch_start_date,
            df_bm.transplant_date,
            df_bm.batch_end_date,
            df_bm.original_population,
            df_bm.loading_datetime,
            df_hm.harvest_variant_name,
            df_hm.vegetable_variety,
            df_vm.vegetable_subcategory
        FROM {JOINED_TABLE_THREE} AS df_bm
    LEFT JOIN {JOINED_TABLE_FOUR} AS df_hm
            ON df_bm.vegetable_variety = df_hm.vegetable_variety
    LEFT JOIN {JOINED_TABLE_SEVEN} AS df_vm
            ON df_vm.vegetable_variant = df_hm.vegetable_variety
    ) AS d1
    LEFT JOIN {JOINED_TABLE_ONE} AS df_inb
        ON df_inb.date BETWEEN d1.transplant_date AND d1.batch_end_date
        AND df_inb.gh_name = d1.gh_name
        AND df_inb.harvest_variant_name = d1.harvest_variant_name
    GROUP BY
        d1.batch_id,
        d1.gh_name,
        d1.vegetable_variety,
        d1.batch_start_date,
        d1.transplant_date,
        d1.vegetable_subcategory,
        d1.batch_end_date,
        d1.original_population
    """
    jb_df = client.query(join_in_between_query).to_dataframe()
    logger.info("Data join completed between batch_master and harvest tables.")

    # Step 3: Left join gh_status_df with jb_df on batch_id and gh_name
    aggregated_df = pd.merge(gh_status_df[['actual_population','batch_id', 'gh_name', 'status', 'start_date', 'end_date','vegetable_variety']], 
                             jb_df[['vegetable_subcategory','batch_start_date', 'original_population','vegetable_variety', 'gh_name', 'batch_id', 'bruto_kg', 'transplant_date', 'batch_end_date']], 
                             on=['batch_id', 'gh_name', 'vegetable_variety'], how='left')
    logger.info("Joined gh_status_df with jb_df on batch_id and gh_name.")
    
    
    # Aggregation Logic with Error Handling
    try:
        aggregated_df = aggregated_df.groupby(
            ['gh_name', 'batch_id'],  # Grouping by gh_name and batch_id
            as_index=False
        ).agg({
            'bruto_kg': 'sum',  # Sum the bruto_kg
            'actual_population': 'first',
            'vegetable_subcategory': 'first',
            'status': 'first',
            'batch_start_date': 'first', 
            'start_date': 'first',
            'end_date': 'first',
            'original_population': 'first',  
            'vegetable_variety': 'first',
            'transplant_date': 'first', 
            'batch_end_date': 'first'
        })
        logger.info("Aggregated rows with identical gh_name and batch_id.")
    except Exception as e:
        logger.error(f"Error during aggregation: {str(e)}")
        raise e  # Raise the caught exception to stop the DAG execution
    
    
    
    # Push the aggregated data to XCom
    kwargs['ti'].xcom_push(key='aggregated_df_one', value=aggregated_df)
    return aggregated_df

def transform_data_two(**kwargs):  # join aggregated_df with gh_master
    """Perform transformation steps on the extracted data, including joining aggregated_df to gh_master."""
    aggregated_df = kwargs['ti'].xcom_pull(task_ids='transform_data_one', key='aggregated_df_one')
    
    client = get_bigquery_client()
    
    # Step 5: Retrieve the gh_master table
    query = f"""
    SELECT 
        gh_code, 
        gh_long_name, 
        area_sqm, 
        no_of_gables, 
        tandon_netsuite, 
        phase_breakdown,
        no_of_polybags,
        tank_assign
    FROM {JOINED_TABLE_TWO}
    """
    gm_df = client.query(query).to_dataframe()
    logger.info("gh_master data retrieved successfully.")
    
    # Step 6: Rename gh_code to gh_name in gm_df
    gm_df.rename(columns={'gh_code': 'gh_name'}, inplace=True)
    
    # Left join aggregated_df with gm_df
    aggregated_df_gh_master = pd.merge(aggregated_df, gm_df[['gh_name', 'gh_long_name', 'phase_breakdown', 'area_sqm', 'no_of_gables', 'tandon_netsuite', 'no_of_polybags','tank_assign']], 
                                       on='gh_name', how='left')
    logger.info("Enriched aggregated_df with gm_df data.")
    
# -- Start of Testing ---
    # Define df_copy
    df_copy = aggregated_df_gh_master.copy()
    
    # Ensure 'df_copy' has the 'original_population' column from 'aggregated_df_gh_master' before performing calculations
    df_copy['original_population'] = aggregated_df_gh_master['original_population']

    # Perform groupby operation on 'df_copy' to calculate the total population per 'gh_name'
    df_copy['total_population'] = df_copy.groupby('gh_name')['original_population'].transform('sum')

    # Calculate 'constant_sqm' in 'df_copy'
    df_copy['constant_sqm'] = df_copy['area_sqm'] / df_copy['total_population']

    # Calculate 'variety_sqm' in the original 'aggregated_df_gh_master' using values from 'df_copy'
    aggregated_df_gh_master['variety_sqm'] = df_copy['constant_sqm'] * aggregated_df_gh_master['original_population']

    logger.info("Calculating variety_sqm")

# -- End of Testing ---

    # Push the final aggregated data to XCom
    kwargs['ti'].xcom_push(key='aggregated_df_two', value=aggregated_df_gh_master)
    return aggregated_df_gh_master

def transform_data_three(**kwargs):  # join aggregated_df with tank_groupping_history
    """Perform transformation steps on the extracted data, including joining aggregated_df to gh_master."""
    aggregated_df = kwargs['ti'].xcom_pull(task_ids='transform_data_two', key='aggregated_df_two')
    
    client = get_bigquery_client()
    
    # Step 5: Retrieve the pic and pic_2 from dds tank_groupping_history as tmgh_df
    query = f"""
        WITH pk_count_query AS (
        SELECT 
            gh_name, 
            farmer_name,
            agronomist,
            block_leader,
            CASE WHEN end_date IS NULL THEN '2099-12-30' ELSE end_date END AS end_date,
            ROW_NUMBER() OVER (PARTITION BY gh_name, farmer_name, agronomist, block_leader ORDER BY end_date) AS row_num
        FROM {JOINED_TABLE_SIX}
        WHERE end_date = '2099-12-30' OR end_date IS NULL
        )
        SELECT 
        gh_name,
        farmer_name,
        end_date,
        agronomist,
        block_leader
        FROM pk_count_query
        WHERE row_num = 1
        ORDER BY gh_name ASC;
    """
    tmgh_df = client.query(query).to_dataframe()
    logger.info("gh_master data retrieved successfully.")
    
    # Do left join aggregated_df with tmgh_df to fill farmer_name on pic
    aggregated_df_tmgh = pd.merge(aggregated_df, tmgh_df[['gh_name', 'farmer_name', 'agronomist', 'block_leader']], 
                                  on='gh_name', how='left')
    logger.info("Enriched aggregated_df_tmgh with farmer_name for pic.")

    # Create new column 'pic' and fill it with 'farmer_name' from tmgh_df
    aggregated_df_tmgh['pic'] = aggregated_df_tmgh['farmer_name']
    
    # Drop the 'farmer_name' column as it's no longer needed
    aggregated_df_tmgh.drop(columns=['farmer_name'], inplace=True)

    # Ensure 'pic' column is treated as object type (string) to hold 'N/A' values
    aggregated_df_tmgh['pic'] = aggregated_df_tmgh['pic'].fillna('N/A').replace('nan', 'N/A')

    # Ensure 'transplant_date' is in datetime format temporarily
    aggregated_df_tmgh['transplant_date'] = pd.to_datetime(aggregated_df_tmgh['transplant_date'])

    # Push the final aggregated data to XCom
    kwargs['ti'].xcom_push(key='aggregated_df_three', value=aggregated_df_tmgh)
    return aggregated_df_tmgh

def transform_data_four(**kwargs):
    """Perform transformation steps on the extracted data, including joining aggregated_df to ideal_yield."""
    aggregated_df = kwargs['ti'].xcom_pull(task_ids='transform_data_three', key='aggregated_df_three')
    
    client = get_bigquery_client()
    
    # Correct table reference
    query = f"""
    SELECT 
        days_from_transplant,
        cumulative_yield
    FROM {JOINED_TABLE_FIVE}  -- Corrected table reference for sakata_hst_ideal_yield
    """
    shiy_df = client.query(query).to_dataframe()
    logger.info("Ideal yield data (sakata_hst_ideal_yield) retrieved successfully.")
    
    aggregated_df['hst'] = None 
    
    
    shiy_df['days_from_transplant'] = shiy_df['days_from_transplant'].astype('int64') 

    
    # Ensure 'today' is a datetime object
    today = pd.to_datetime("today")
    
    # Perform the subtraction to calculate 'diff_days'
    aggregated_df['hst'] = (today - aggregated_df['transplant_date']).dt.days
    
    # Add column remaining_days
    aggregated_df['remaining_days'] = 360 - aggregated_df['hst']
    logger.info("Calculated 'remaining_days' as 360 - hst.")
    
    # Convert back the transplant_date to 'DATE' without time part
    aggregated_df['transplant_date'] = aggregated_df['transplant_date'].dt.date
    
    # Merge aggregated_df with shiy_df to retrieve the cumulative yield
    aggregated_df_shiy = pd.merge(aggregated_df, shiy_df[['days_from_transplant', 'cumulative_yield']],
                                  left_on='hst', right_on='days_from_transplant', how='left')

   
    aggregated_df_shiy['days_from_transplant'] = aggregated_df_shiy['days_from_transplant'].replace([np.inf, -np.inf], np.nan)
    aggregated_df_shiy['days_from_transplant'] = aggregated_df_shiy['days_from_transplant'].fillna(0).astype('int64')  
    
   
    # Drop the 'cumulative_yield' column
    aggregated_df_shiy['days_from_transplant'] = aggregated_df_shiy['days_from_transplant'].astype('int64')

    # Push the final aggregated data to XCom
    kwargs['ti'].xcom_push(key='aggregated_df_four', value=aggregated_df_shiy)
    return aggregated_df_shiy  # Return the correct DataFrame

# ---- Start of Testing
def data_enriching(**kwargs):
    """Perform data enriching steps on aggregated_df, including rough calculation or Hard Code."""
    aggregated_df_de = kwargs['ti'].xcom_pull(task_ids='transform_data_four', key='aggregated_df_four')
    
    client = get_bigquery_client()
    
    # Additional Logic (For unify naming to Big Chili)
    aggregated_df_de['vegetable_subcategory'] = aggregated_df_de['vegetable_subcategory'].replace({
        r'(Green|Red) Big Chili.*': 'Big Chili',  # Regex to match any variation of Green/Red Big Chili
        r'(Green|Red) Rawit Chili.*': 'Rawit Chili'  # Regex to match any variation of Green/Red Rawit Chili
    }, regex=True)    
    
    logger.info("Updated 'vegetable_subcategory' column to consolidate values 'Green Big Chili Sakata' and 'Red Big Chili Sakata' to 'Big Chili Sakata'.")

    # Step X: Add ideal yield calculation
    def calculate_ideal_yield(row):
        # Ensure 'harvest_variant_name' is a string and handle NaN value
        vegetable_subcategory = ['vegetable_subcategory']
        if pd.isna(row['vegetable_subcategory']) or not isinstance(row['vegetable_subcategory'], str):
            return 0
        if 'Big Chili' in row['vegetable_subcategory']:
            return (70000 / 10000) * row['variety_sqm']
        elif 'Rawit Chili' in row['vegetable_subcategory']:
            return (45000 / 10000) * row['variety_sqm']
        elif 'Tomato Cherry' in row['vegetable_subcategory']:
            return (60000 / 10000) * row['variety_sqm']
        return 0  # Default case

    # Data enrichment additional columns
    aggregated_df_de['ideal_yield_kg'] = aggregated_df_de.apply(calculate_ideal_yield, axis=1)
    aggregated_df_de['yield_per_plant'] = aggregated_df_de['bruto_kg'] / aggregated_df_de['actual_population']
    aggregated_df_de['productivity_per_ideal_yield'] = np.where(aggregated_df_de['ideal_yield_kg']==0, 0, aggregated_df_de['bruto_kg'] / aggregated_df_de['ideal_yield_kg'])
    aggregated_df_de['hst_ideal_yield_per_polybag_kg'] = aggregated_df_de['cumulative_yield']
    
    # Drop Cumulative Yield
    aggregated_df_de.drop(columns=['cumulative_yield'], inplace = True)
    aggregated_df_de['hst_ideal_yield_kg'] = aggregated_df_de['actual_population'] * aggregated_df_de['hst_ideal_yield_per_polybag_kg']
    
    aggregated_df_de['productivity_per_hst_ideal_yield'] = aggregated_df_de['bruto_kg'] / aggregated_df_de['hst_ideal_yield_kg']
    logger.info("Calculated 'productivity_per_hst_ideal_yield'.")
    
    # More Additional Column
    aggregated_df_de['seeding_date'] = None
    aggregated_df_de['days_after_seeding'] = None
    
    aggregated_df_de['seeding_date'] = aggregated_df_de['batch_start_date'] # INT (Seeding Date di Batch Master)
    aggregated_df_de['days_after_seeding'] = aggregated_df_de['days_after_seeding'].fillna('2099-12-31') # INT (Date Diff Batch_Start_Date - Transplant_Date)
    
# --- Start Testing ---    

    aggregated_df_de['actual_area_sqm'] = (
        (aggregated_df_de['original_population'] / aggregated_df_de['no_of_polybags'].replace(0, np.nan)) 
        * aggregated_df_de['area_sqm']
    )
    aggregated_df_de['actual_gable'] = (
        (aggregated_df_de['original_population'] / aggregated_df_de['no_of_polybags'].replace(0, np.nan)) 
        * aggregated_df_de['no_of_gables']
    )
    
    aggregated_df_de['gh_population'] = aggregated_df_de['no_of_polybags']
    aggregated_df_de.drop(columns=['no_of_polybags'], inplace = True)
    
# ---- End of Testing

    # Push the final aggregated data to XCom
    kwargs['ti'].xcom_push(key='aggregated_df_final', value=aggregated_df_de)
    return aggregated_df_de  # Return the correct DataFrame

def truncate_data(**kwargs):
    """Truncate the target BigQuery table before loading new data."""
    client = get_bigquery_client()
    client.query(f"TRUNCATE TABLE {TARGET_TABLE}").result()
    logger.info(f"Table {TARGET_TABLE} truncated successfully.")

def load_data(**kwargs):
    """Load the transformed data into the target BigQuery table."""
    # Pull aggregated data from XCom
    final_aggregated_df = kwargs['ti'].xcom_pull(task_ids='data_enriching', key='aggregated_df_final')

    if final_aggregated_df is None or final_aggregated_df.empty:
        logger.error("No data to load into BigQuery.")
        raise ValueError("No data to load into BigQuery.")

    # Log the number of records to be loaded
    logger.info(f"Loading data with {len(final_aggregated_df)} records.")
    
    # Log data sample before loading
    logger.info(f"Data sample to load:\n{final_aggregated_df.head()}")

    # Load data into BigQuery
    client = get_bigquery_client()
    final_aggregated_df.to_gbq(destination_table=TARGET_TABLE, 
                          project_id='biap-datainfra-gcp', 
                          if_exists='replace', 
                          credentials=client._credentials)
    logger.info("Data loaded into BigQuery table successfully.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='t1_gh_hardy_allocation_dag',
    default_args=default_args,
    schedule_interval='0 11 * * *',  # Run daily at 11 AM UTC
    catchup=False,
    tags=['t1_ckp_gh_hardy_alloc_dvm']
) as dag:

    # Task definitions
    extract_data_task = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data_task_one = PythonOperator(task_id='transform_data_one', python_callable=transform_data_one)
    transform_data_task_two = PythonOperator(task_id='transform_data_two', python_callable=transform_data_two)
    transform_data_task_three = PythonOperator(task_id='transform_data_three', python_callable=transform_data_three)
    transform_data_task_four = PythonOperator(task_id='transform_data_four', python_callable=transform_data_four)
    data_enriching = PythonOperator(task_id='data_enriching', python_callable=data_enriching)
    truncate_data_task = PythonOperator(task_id='truncate_data', python_callable=truncate_data)
    load_data_task = PythonOperator(task_id='load_data', python_callable=load_data)

    # Set task dependencies
    extract_data_task >> transform_data_task_one >> transform_data_task_two >> transform_data_task_three >> transform_data_task_four >> data_enriching >> truncate_data_task >> load_data_task
