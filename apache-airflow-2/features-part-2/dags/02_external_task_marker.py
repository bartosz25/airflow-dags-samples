"""
This DAG simulates a real-world scenario of a data generator and a data consumer. The generator produces the data
that is later exposed from a BI dashboard.

The dependency between DAGs is expressed with `ExternalTaskMarker` and `ExternalTaskSensor`. The marker is there to
ensure automatic reprocessing of the consumer in case of the backfilling.
"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

with DAG(dag_id="data_generator", schedule='@daily', start_date=datetime(2022, 11, 4)) as data_generator_dag:

    @task
    def generate_data():
        print('Generating data')

    parent_task = ExternalTaskMarker(
        task_id="dashboard_refresher_trigger",
        external_dag_id="bi_dashboard_refresher",
        external_task_id="data_generator_sensor",
    )

    generate_data() >> parent_task

data_generator_dag

with DAG(dag_id="bi_dashboard_refresher", schedule='@daily', start_date=datetime(2022, 11, 4)) as bi_dashboard_refresher_dag:

    data_generator_sensor = ExternalTaskSensor(
        task_id='data_generator_sensor',
        external_dag_id='data_generator',
        external_task_id='dashboard_refresher_trigger',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",
    )

    @task
    def refresh_dashboard():
        print('Refreshing dashboard')


    data_generator_sensor >> refresh_dashboard()

bi_dashboard_refresher_dag