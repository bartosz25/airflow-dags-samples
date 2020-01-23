from airflow import utils
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='pattern_parallel_split',
    default_args={
        'start_date': utils.dates.days_ago(1),
    },
    schedule_interval=None,
)

with dag:
    read_input = DummyOperator(task_id='read_input')

    aggregate_data = DummyOperator(task_id='generate_data')

    convert_to_parquet = DummyOperator(task_id='convert_to_parquet')

    convert_to_avro = DummyOperator(task_id='convert_to_avro')

    read_input >> aggregate_data >> [convert_to_parquet, convert_to_avro]
