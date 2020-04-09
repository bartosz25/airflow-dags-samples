import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeDeltaSensor

from printer import print_execution_date

dag = DAG(
    dag_id='new_delta_sensor',
    schedule_interval='@hourly',
    start_date=datetime.datetime(2020, 4, 9, 0, 0, 0)
)

with dag:
    guard_sensor = TimeDeltaSensor(
        task_id='guard_sensor',
        delta=datetime.timedelta(hours=2),  # let 3 more hours for late data to arrive (2 + 1h since DAG executes at the end of the schedule interval)
        poke_interval=60*10,  # 10 minutes
        mode='reschedule'
    )
    printer = PythonOperator(
        task_id='printer',
        python_callable=print_execution_date,
        provide_context=True
    )
    guard_sensor >> printer
