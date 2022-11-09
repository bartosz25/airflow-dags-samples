import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from printer import print_execution_date

dag = DAG(
    dag_id='old_delta_sensor',
    schedule_interval='@hourly',
    start_date=datetime.datetime(2020, 4, 9, 0, 0, 0)
)

with dag:
    def generate_dates(**context):
        execution_date = context['execution_date']
        timestamped_partition_to_load = execution_date - datetime.timedelta(hours=3)
        return timestamped_partition_to_load

    timeshift_date = PythonOperator(
        task_id='timeshift_date',
        provide_context=True,
        python_callable=generate_dates
    )
    printer = PythonOperator(
        task_id='printer',
        python_callable=print_execution_date,
        provide_context=True
    )
    timeshift_date >> printer
