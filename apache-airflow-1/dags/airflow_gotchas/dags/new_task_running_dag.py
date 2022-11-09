from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='new_task_for_running_dag',
    start_date=datetime(2020, 1, 3, 0, 0),
    schedule_interval='@hourly'
)

with dag:
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')
    task_3bis = DummyOperator(task_id='task_3bis')
    task_4 = DummyOperator(task_id='task_4')

    task_1 >> task_2 >> task_3 >> task_3bis >> task_4
