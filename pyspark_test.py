from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
import logging

# Function to create a Spark session and perform a simple operation
def run_pyspark_task():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .getOrCreate()

    # Create a simple DataFrame
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    columns = ["name", "value"]
    df = spark.createDataFrame(data, columns)

    # Perform a simple action, such as count the rows
    row_count = df.count()

    # Log the result
    logging.info(f"Number of rows in DataFrame: {row_count}")

    # Stop the Spark session
    spark.stop()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'pyspark_test_dag',  # Name of the DAG
    default_args=default_args,
    description='A simple PySpark test DAG',
    schedule_interval=None,  # Set to None to trigger manually or on-demand
    catchup=False,  # Avoid backfilling
)

# Define the task to run PySpark job
pyspark_task = PythonOperator(
    task_id='run_pyspark_job',
    python_callable=run_pyspark_task,
    dag=dag,
)

# Set the task dependencies (only one task here, so no dependencies)
pyspark_task
