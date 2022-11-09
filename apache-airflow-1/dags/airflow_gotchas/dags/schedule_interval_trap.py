from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='schedule_interval_trap',
    start_date=datetime(2020, 1, 24, 0, 0),
    schedule_interval='0 0 24 * *'
)

with dag:
    task_1 = DummyOperator(task_id='task_1')