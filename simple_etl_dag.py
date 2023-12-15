from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jm',
    'start_date': datetime(2023, 12, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def extract_data(**kwargs):
    print("Extracting data...")
    return {"data": [1, 2, 3]}

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    transformed_data = [item * 2 for item in data['data']]
    print("Transforming data...")
    return {"transformed_data": transformed_data}

def load_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    print("Loading data...")
    print("Transformed Data:", transformed_data)

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

start_task >> extract_task >> transform_task >> load_task >> end_task
