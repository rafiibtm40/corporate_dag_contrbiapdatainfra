from airflow import DAG
from airflow.models import XCom
from airflow.utils.session import provide_session
from datetime import datetime

@provide_session
def cleanup_xcom(dag_id, execution_date, session=None):
    """
    Cleanup XCom data for the given DAG and execution date.
    """
    session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.execution_date == execution_date
    ).delete(synchronize_session='fetch')
    
    session.commit()

def on_success_callback(context):
    """
    Callback function to call after a successful DAG run.
    """
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    cleanup_xcom(dag_id, execution_date)

