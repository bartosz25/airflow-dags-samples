from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='start_date_now',
    default_args={
        'start_date': datetime.now(),
    },
    schedule_interval='*/1 * * * *',
)

with dag:
    DummyOperator(task_id='task_1') >> DummyOperator(task_id='task_2')
