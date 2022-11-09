from airflow import utils
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta
dag = DAG(
    dag_id='pattern_sequential',
    default_args={
        'start_date': utils.dates.days_ago(1),
    },
    schedule_interval=None
)

with dag:
    read_input = DummyOperator(task_id='read_input')

    aggregate_data = DummyOperator(task_id='generate_data')

    write_to_redshift = DummyOperator(task_id='write_to_redshift')

    read_input >> aggregate_data >> write_to_redshift
