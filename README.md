# corporate_dag_contrbiapdatainfra

1// Project Title:
Cikampek Data Pipeline (Batch Processing)

2// Overview:
This DAG is designed to extract, transform, and load (ETL) the daily farm data from our transaction database into our data warehouse in Bigquery (BQ). The goal is to provide up-to-date reporting for the sales team and executives. The pipeline includes the usage of Gsheet directly via appscript load to BQ. The setup of BQ itself if we categorize it by the data cleanliness, the category is: staging (stg) and data definition stage (dds), then data visualization mart (dvm). in dds the data is checked PK and FK so it will be cleaner.

But if we categorize it with the bq structure of testing purposes, we have staging and dev environment that indicates by the _stg and _dev after the site.


3//Prerequisites
- Apache Airflow 2.0+ (or specific version)
- Python 3.8+
- GCP credentials (for accessing Google Cloud Storage, BigQuery, etc.)
- GCP Virtual Machine (Host it without docker)
- Required Python libraries: `pandas`, `requests`, `google-cloud`

4//Setup Instuction
1. Clone the repository into your local Airflow project directory.
2. Set up the following Airflow connections: (please make sure you have already activate the gsutil package)
   - Google Cloud Storage (GCS)
   - BigQuery
3. Ensure that your `airflow.cfg` file contains the proper configurations for your environment.
4. Install any required Python libraries by running `pip install -r requirements.txt`.


5// Dag Structure and Components
The DAG consists of the following steps:
- **Extract Data**: A PythonOperator that extracts raw sales data from our transactional database.
- **Transform Data**: A PythonOperator that cleans and transforms the raw data using `pandas`.
- **Load Data to BigQuery**: A BigQueryOperator that uploads the cleaned data to a staging table in BigQuery.
- **Error Handling**: Tasks that notify the team in case of any failures in the process.


6// Configuration
- **dag_id**: `daily_sales_report_etl`
- **start_date**: `2022-01-01`
- **schedule_interval**: `0 6 * * *` (Every day at 6 AM)
- **retries**: `3`
- **retry_delay**: `5 minutes`


7// How to Trigger dag
The DAG is scheduled to run automatically every day at 6 AM. To manually trigger it, use the following command:
airflow dags trigger daily_sales_report_etl


8// Logs and Monitoring
Logs for each task are available in the Airflow UI under the "Logs" section for the respective DAG run. In case of failure, a Slack notification will be sent to the team.


9// Error Handling
If a task fails, the DAG will retry up to 3 times with a delay of 5 minutes between retries. If the task continues to fail, a Slack notification will be sent to the team for further action.


10// Troubleshooting
**Q: The DAG is not triggering.**
A: Ensure the start date is set correctly and that the scheduler is running.

**Q: I see an error related to missing dependencies.**
A: Make sure you have installed the required dependencies from `requirements.txt`.


11// Contributing
If you'd like to contribute to the DAG, please fork the repository, make your changes, and submit a pull request. Ensure that all tasks are well-documented and that the DAG runs successfully in the Airflow UI.


12// License
null

