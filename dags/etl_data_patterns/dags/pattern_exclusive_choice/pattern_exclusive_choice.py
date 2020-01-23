import random

from airflow import utils
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

dag = DAG(
    dag_id='pattern_exclusive_choice',
    default_args={
        'start_date': utils.dates.days_ago(1),
    },
    schedule_interval=None,
)

with dag:
    def route_task():
        if random.randint(0, 100) % 2 == 0:
            return 'convert_to_parquet'
        else:
            return 'convert_to_avro'

    read_input = DummyOperator(task_id='read_input')

    aggregate_data = DummyOperator(task_id='generate_data')

    route_to_format = BranchPythonOperator(task_id='route_to_format', python_callable=route_task)

    convert_to_parquet = DummyOperator(task_id='convert_to_parquet')

    convert_to_avro = DummyOperator(task_id='convert_to_avro')

    read_input >> aggregate_data >> route_to_format >> [convert_to_parquet, convert_to_avro]
