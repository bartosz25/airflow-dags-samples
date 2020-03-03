from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor

dag_source = DAG(
    dag_id='dag_sensor_source',
    start_date=datetime(2020, 1, 24, 0, 0),
    schedule_interval='* * * * *'
)

task_1 = DummyOperator(task_id='task_1', dag=dag_source)


dag_target = DAG(
    dag_id='dag_sensor_target',
    start_date=datetime(2020, 1, 24, 0, 0),
    schedule_interval='* * * * *'
)
task_sensor = ExternalTaskSensor(
    dag=dag_target,
    task_id='dag_sensor_source_sensor',
    retries=100,
    retry_delay=timedelta(seconds=30),
    mode='reschedule',
    external_dag_id='dag_sensor_source',
    external_task_id='task_1'
)

task_1 = DummyOperator(task_id='task_1', dag=dag_target)

task_sensor >> task_1