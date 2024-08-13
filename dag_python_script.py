from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from Extraction import run_extraction
from Loading import run_loading
from Transformation import run_transformation

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dag(dag_id, schedule, default_args, catchup=False):
    # Define the DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description='A simple DAG',
        schedule_interval=schedule,
        catchup=catchup,
    )

    # Define tasks
    extraction_task = PythonOperator(
        task_id='extraction_id',
        python_callable=run_extraction,
        dag=dag
    )

    transformation_task = PythonOperator(
        task_id='transformation_id',
        python_callable=run_transformation,
        dag=dag
    )

    loading_task = PythonOperator(
        task_id='loading_id',
        python_callable=run_loading,
        dag=dag
    )

    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )

    # Set task dependencies
    extraction_task >> transformation_task >> loading_task >> end_task

    return dag

# Create a DAG instance
dag_id = 'zipco_food_orchestration'
schedule = '@daily'
dag = create_dag(dag_id, schedule, default_args)
