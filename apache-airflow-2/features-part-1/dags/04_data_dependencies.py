"""
Data dependencies aka data-aware scheduling are like event-based triggers. They start DAG execution when the trigger
dataset is created or modified.
"""
import os
from datetime import datetime

from airflow import Dataset, DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from pendulum import DateTime

start_date_for_reader_and_writer = datetime(2022, 11, 4)
dataset_path = 'file:///tmp/datasets'
os.makedirs(dataset_path, exist_ok=True)
dataset = Dataset(uri=dataset_path)

with DAG(dag_id='dataset_writer', start_date=start_date_for_reader_and_writer, schedule='@daily') as writing_dag:

    @task(outlets=[dataset])
    def data_generator():
        context = get_current_context()
        execution_date: DateTime = context['execution_date']
        with open(f'{dataset_path}/{execution_date.to_date_string()}.txt', 'w') as file_to_write:
            file_to_write.write("abc")

    data_generator()

with DAG(dag_id='dataset_reader', start_date=start_date_for_reader_and_writer, schedule=[dataset]) as reading_dag:

    @task
    def data_consumer():
        print(os.listdir(dataset_path))

    data_consumer()

writing_dag
reading_dag