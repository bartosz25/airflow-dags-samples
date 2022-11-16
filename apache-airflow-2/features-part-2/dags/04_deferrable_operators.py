"""
In this example I'm addressing an unpredictable data generation use case. Usually, we would solve it with a
sensor. However, Apache Airflow 2 offers a lighter solution of Deferrable operators that I'm demonstrating just below.

The triggers are managed by a component called `Triggerer`. You must start it (e.g. `airflow triggerer`) before using
the triggers. Otherwise, they won't be queued.
"""
import asyncio
import logging
import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context


class SuccessFileTrigger(BaseTrigger):

    def __init__(self, file_input_directory: str):
        super().__init__()
        self.file_found = False
        self.file_input_directory = file_input_directory
        self.file_to_wait_for = f'{file_input_directory}/_SUCCESS'

    def _check_if_file_was_created(self):
        logging.info(f'Checking if {self.file_to_wait_for} exists...')
        if os.path.exists(self.file_to_wait_for):
            logging.info('...found file')
            self.file_found = True
        else:
            logging.info('...not yet')

    async def run(self):
        while not self.file_found:
            self._check_if_file_was_created()
            await asyncio.sleep(5)
        yield TriggerEvent(self.file_to_wait_for)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("04_deferrable_operators.SuccessFileTrigger", {"file_input_directory": self.file_input_directory})


class SparkSuccessFileSensor(BaseSensorOperator):

    template_fields = ['file_input_directory']

    def __init__(self, file_input_directory: str, **kwargs):
        super().__init__(**kwargs)
        self.file_input_directory = file_input_directory

    def execute(self, context: Context) -> Any:
        self.defer(trigger=SuccessFileTrigger(self.file_input_directory), method_name='mark_sensor_as_completed')

    def mark_sensor_as_completed(self, context: Context, event: str):
        logging.info(f'Got event answer={event}')
        return event

# TODO: change start date to yesterday to always have a single DAG instance to run; otherwise reading logs may be hard
with DAG(dag_id="spark_job_data_consumer", schedule='@daily', start_date=datetime(2022, 11, 15)) as spark_job_data_consumer:

    success_file_sensor = SparkSuccessFileSensor(
        task_id='success_file_sensor',
        file_input_directory='/tmp/spark_job_data/{{ ds_nodash }}',
        poke_interval=5
    )

    @task
    def process_data():
        print('Generating data')

    success_file_sensor >> process_data()

spark_job_data_consumer
