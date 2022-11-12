"""
The DAG shows new features of the Task Flow API, including:

- @task decorator instead of declaring an Operator every time
- XCom variable usage without calling the push/pull methods
- a combination with the task not supported by the @task decorator (but can be extended with
  https://airflow.apache.org/docs/apache-airflow/2.4.2/howto/create-custom-decorator.html)
- fan-out dependencies declaration

In my example I'm writing a dummy representation of an ETL pipeline with an output copied to 2 different stores.
"""
import logging

from airflow.decorators import dag, task
from datetime import datetime

import requests
import json

from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.sensors.python import PythonSensor

url = 'https://covidtracking.com/api/v1/states/'
state = 'wa'

default_args = {
    'start_date': datetime(2022, 1, 1)
}

@dag('logs_generator', schedule_interval='@daily', default_args=default_args, max_active_runs=1, catchup=False)
def create_dag_with_taskflow_api():

    def check_files_availability():
        context = get_current_context()
        task_instance: TaskInstance = context['task_instance']
        task_instance.xcom_push('file_location', 's3://somewhere/data/....')
        return True

    data_availability_checker = PythonSensor(
        task_id='data_availability_checker',
        python_callable=check_files_availability,
        do_xcom_push=True
    )

    @task
    def create_cluster():
        task_definition = {'jar': 'abc.jar'}
        logging.info(f'Creating cluster with {task_definition}')
        return 'cluster_id'

    @task
    def notify_running_job(cluster_id: str):
        logging(f'A job is running on the cluster {cluster_id}')
        return 'job_id_a'

    def check_job_status():
        context = get_current_context()
        task_instance: TaskInstance = context['task_instance']
        job_id = task_instance.xcom_pull(task_ids='notify_running_job')
        logging.info(f'Checking job status for {job_id}')
        return True

    job_execution_waiter = PythonSensor(
        task_id='job_execution_waiter',
        python_callable=check_job_status
    )

    @task
    def s3_copy_maker():
        logging('Copying files to S3')

    @task
    def gcs_copy_maker():
        logging('Copying files to GCS')

    @task
    def azure_storage_copy_maker():
        logging('Copying files to Azure Storage')

    cluster_creator = create_cluster()
    data_availability_checker >> cluster_creator >> notify_running_job(cluster_creator) >> job_execution_waiter >> [s3_copy_maker(), gcs_copy_maker(), azure_storage_copy_maker()]
    #data_availability_checker >> cluster_creator >> job_execution_waiter


dag = create_dag_with_taskflow_api()