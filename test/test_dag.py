import pytest
from unittest.mock import MagicMock
from airflow.models import DagBag
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

# This assumes your DAG file is named "t1_upsert_gh_status_dds.py"
@pytest.fixture(scope='module')
def dagbag():
    """
    Load the DAG from the DAG directory.
    """
    dagbag = DagBag(dag_folder='corporate_dag_contrbiapdatainfra')
    return dagbag

def test_dag_loaded(dagbag):
    """
    Test to ensure the DAG is loaded correctly.
    """
    dag = dagbag.get_dag('t1_upsert_gh_status_dds')
    assert dag is not None
    assert len(dag.tasks) > 0

def test_extract_data(dagbag, mocker):
    """
    Test the `extract_data` task in the DAG.
    Mocks the BigQuery client and returns a sample dataframe.
    """
    # Mocking the BigQuery client and query response
    mock_client = MagicMock(spec=bigquery.Client)
    mock_df = pd.DataFrame({
        'batch_id': [1, 2],
        'gh_name': ['test_gh1', 'test_gh2'],
        'status': ['active', 'inactive'],
        'start_date': ['2024-01-01', '2024-01-02'],
        'end_date': ['2024-01-31', '2024-02-28'],
        'vegetable_variety': ['variety1', 'variety2'],
        'leader': ['leader1', 'leader2'],
        'pic': ['pic1', 'pic2'],
        'pic_2': ['pic2', 'pic3'],
        'actual_harvest': [100, 200],
        'reason_to_exterminate': ['reason1', 'reason2'],
        'remarks': ['remark1', 'remark2'],
        'actual_population': [500, 600],
    })

    # Mocking the query to return the above DataFrame
    mock_client.query.return_value.to_dataframe.return_value = mock_df
    
    # Injecting the mock client into the function (bypassing the actual BigQuery client)
    mocker.patch('your_dag_module.get_bigquery_client', return_value=mock_client)
    
    # Extract data using the mocked client
    from your_dag_module import extract_data
    result = extract_data(service_account_path='mock/path/to/key.json')

    # Assert that the extracted data is the same as our mock data
    pd.testing.assert_frame_equal(result, mock_df)

def test_transform_data(dagbag, mocker):
    """
    Test the `transform_data` task in the DAG.
    Mocks the XCom pull and simulates the transformation logic.
    """
    # Sample input data that will be pulled by XCom
    input_data = pd.DataFrame({
        'batch_id': [1, 2],
        'gh_name': ['test_gh1', 'test_gh2'],
        'status': ['active', 'inactive'],
        'start_date': ['2024-01-01', '2024-01-02'],
        'end_date': ['2024-01-31', '2024-02-28'],
        'vegetable_variety': ['variety1', 'variety2'],
        'leader': ['leader1', 'leader2'],
        'pic': ['pic1', 'pic2'],
        'pic_2': ['pic2', 'pic3'],
        'actual_population': [500, 600],
    })

    # Mocking XCom pull
    mocker.patch('airflow.operators.python.PythonOperator.xcom_pull', return_value=input_data)

    # Run the transform_data task
    from your_dag_module import transform_data
    transformed_data, _ = transform_data(service_account_path='mock/path/to/key.json', ti=MagicMock())
    
    # Verify some transformations (for simplicity, check the shape)
    assert transformed_data.shape == input_data.shape
    assert 'loading_datetime' in transformed_data.columns  # Ensure the new column is added

def test_load_data(dagbag, mocker):
    """
    Test the `load_passed_data` task in the DAG.
    Mocks the BigQuery load function to avoid actual data loading.
    """
    # Mocking the BigQuery client and load method
    mock_client = MagicMock(spec=bigquery.Client)
    mock_client.load_table_from_dataframe.return_value = None

    # Inject the mock BigQuery client
    mocker.patch('your_dag_module.get_bigquery_client', return_value=mock_client)
    
    # Sample passed data to load
    passed_data = pd.DataFrame({
        'batch_id': [1, 2],
        'gh_name': ['test_gh1', 'test_gh2'],
        'status': ['active', 'inactive'],
        'start_date': ['2024-01-01', '2024-01-02'],
        'end_date': ['2024-01-31', '2024-02-28'],
        'vegetable_variety': ['variety1', 'variety2'],
        'leader': ['leader1', 'leader2'],
        'pic': ['pic1', 'pic2'],
        'pic_2': ['pic2', 'pic3'],
        'actual_population': [500, 600],
    })

    # Mock XCom pull to return passed data
    mocker.patch('airflow.operators.python.PythonOperator.xcom_pull', return_value=passed_data)
    
    # Run the load_passed_data task
    from your_dag_module import load_passed_data
    load_passed_data(service_account_path='mock/path/to/key.json', ti=MagicMock())

    # Ensure the load method was called
    mock_client.load_table_from_dataframe.assert_called_once()

