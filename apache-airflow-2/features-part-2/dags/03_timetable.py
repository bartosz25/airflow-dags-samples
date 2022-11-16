"""
By default, Airflow schedules each DAG instance in the end of the defined schedule interval:

"
A DAG run is usually scheduled after its associated data interval has ended, to ensure the run is able to
collect all the data within the time period. In other words, a run covering the data period of 2020-01-01
generally does not start to run until 2020-01-01 has ended, i.e. after 2020-01-02 00:00:00.
"
Source: https://airflow.apache.org/docs/apache-airflow/2.4.2/dag-run.html

This subtlety is less visible in daily schedules but is fortunately, it's not the single schedule possible in Airflow.
For example, you can also need to schedule a DAG during the weekdays only and in that scenario:
- Monday execution runs on Tuesday midnight
- Tuesday execution runs on Wednesday midnight
- Wednesday execution runs on Thursday midnight
- Thursday execution runs on Friday midnight
- Friday executions runs on...next Monday midnight because it's the next date from the defined weekdays schedule

Apache Airflow 2 addresses this issue with customizable schedules called `Timetable`.

In my example, I want to use an opposite mechanism to Airflow scheduler and create the DAG run as soon as the defined
time is reached. For example, to start a run for 1:00, I don't want to wait 2:00. Instead, I want to trigger it
as soon as possible, ideally at 1:00.

In case of errors, you can find logs in `cat logs/scheduler/latest/03_timetable.py.log`
"""
from __future__ import annotations

from datetime import datetime

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from end_of_schedule_timetable import EndOfScheduleTimetable
from pendulum import DateTime

with DAG(dag_id="end_of_schedule_processor", start_date=pendulum.datetime(2022, 11, 14, tz="UTC"), catchup=True,
         timetable=EndOfScheduleTimetable()) as end_of_schedule_processor_dag:

    @task
    def generate_data():
        context = get_current_context()
        execution_date: DateTime = context['execution_date']
        print(f'Generating data at {execution_date}')

    generate_data()

end_of_schedule_processor_dag