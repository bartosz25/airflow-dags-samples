import os

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator


class PokeCountBasedSensor(BaseSensorOperator):
    def __init__(self, pokes_number, *args, **kwargs):
        super(PokeCountBasedSensor, self).__init__(*args, **kwargs)
        self.pokes_number = pokes_number

    def poke(self, context):
        # To create the file:
        # docker exec -ti control_flows_webserver_1 bash
        # touch /tmp/file_to_process.txt
        if os.path.exists('/tmp/file_to_process.txt'):
            print('Job terminated!')
            return True
        print("File /tmp/file_to_process.txt doesn't exists. Retrying.")
        return False


dag = DAG(
    dag_id='until_example',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    max_active_runs=1
)

with dag:
    cluster_creator = DummyOperator(
        task_id='cluster_creator'
    )
    job_sender = DummyOperator(
        task_id='job_sender'
    )
    job_sensor = PokeCountBasedSensor(
        task_id='job_sensor',
        pokes_number=2,
        mode='reschedule',
        poke_interval=2  # retry every 2 seconds
    )
    cluster_cleaner = DummyOperator(
        task_id='cluster_cleaner'
    )

    cluster_creator >> job_sender >> job_sensor >> cluster_cleaner

