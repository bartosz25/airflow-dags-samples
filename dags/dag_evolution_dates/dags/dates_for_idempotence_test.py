from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from pendulum import datetime

task_2019_start = datetime(2019, 11, 1, hour=0)
task_2020_end = datetime(2021, 1, 1, hour=0)
dag = DAG(
    dag_id='dates_for_idempotence_test',
    schedule_interval='@monthly',
    start_date=task_2019_start,
    end_date=task_2020_end
)

with dag:
    task_2019_end = datetime(2019, 12, 31, hour=23)
    common_start = DummyOperator(
        task_id='common_start',
        start_date=task_2019_start,
        end_date=task_2020_end
    )

    task_1_2019 = DummyOperator(
        task_id='task_1_2019',
        start_date=task_2019_start,
        end_date=task_2019_end
    )
    task_2_2019 = DummyOperator(
        task_id='task_2_2019',
        start_date=task_2019_start,
        end_date=task_2019_end
    )

    task_2020_start = datetime(2020, 1, 1, hour=0)
    task_1_2020 = DummyOperator(
        task_id='task_1_2020',
        start_date=task_2020_start,
        end_date=task_2020_end
    )
    task_2_2020 = DummyOperator(
        task_id='task_2_2020',
        start_date=task_2020_start,
        end_date=task_2020_end
    )

    common_start >> [task_1_2019, task_1_2020]
    task_1_2019 >> task_2_2019
    task_1_2020 >> task_2_2020
