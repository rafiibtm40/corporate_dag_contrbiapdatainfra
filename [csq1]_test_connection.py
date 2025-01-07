from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# The SQL statement to execute (creating a table)
create_table_sql = """
CREATE TABLE IF NOT EXISTS my_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);
"""

# Create DAG instance
with DAG(
    'postgres_example_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Specify the interval (daily in this case)
    catchup=False,
) as dag:
    
    # PostgresOperator task to run the SQL command
    create_table = PostgresOperator(
        task_id='create_table',
        sql=create_table_sql,
        postgres_conn_id='postgres_default',  # This should match your connection ID
    )

    # Set up the task
    create_table
