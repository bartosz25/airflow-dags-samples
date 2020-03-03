from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='new_start_date_running_dag',
    default_args={
        'start_date': datetime(2019, 1, 1, 0), # set to 2019
    },
    schedule_interval='@daily',
)

with dag:
    DummyOperator(task_id='task_1') >> DummyOperator(task_id='task_2')
