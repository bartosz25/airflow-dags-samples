from airflow import utils
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='pattern_synchronization',
    default_args={
        'start_date': utils.dates.days_ago(1),
    },
    schedule_interval=None,
)

with dag:
    convert_to_parquet = DummyOperator(task_id='convert_to_parquet')
    for hour in range(0, 24):
        read_input = DummyOperator(task_id='read_input_hour_{}'.format(hour))

        aggregate_data = DummyOperator(task_id='generate_data_hour_{}'.format(hour))

        read_input >> aggregate_data >> convert_to_parquet
