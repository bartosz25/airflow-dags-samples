from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator

file_sensor_dag = DAG(
    dag_id='data_sensor',
    start_date=datetime(2020, 1, 24, 0, 0),
    schedule_interval='* * * * *'
)


file_sensor = FileSensor(
    task_id='minute_file_sensor',
    dag=file_sensor_dag,
    filepath="/tmp/test_file.txt",
    retries=100,
    retry_delay=timedelta(seconds=30),
    mode='reschedule'
)

task_1 = DummyOperator(task_id='task_1', dag=file_sensor_dag)

file_sensor >> task_1