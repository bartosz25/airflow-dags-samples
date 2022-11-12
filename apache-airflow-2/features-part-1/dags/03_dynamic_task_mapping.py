"""
Dynamic task mapping can create a DAG with variable number of tasks among  DAG Runs. Here I'm creating a DAG
with the task number equal to the number of day from the execution date.

In the example we can imagine that we're running pipeline updating tables with late data. The update is limited
to a single month.
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from pendulum import DateTime

with DAG(dag_id="late_data_integration", schedule='@daily', start_date=datetime(2022, 11, 4)) as dag:

    @task
    def refresh_table(table_name: str):
        logging.info(f'Refreshing {table_name}')
        return table_name

    @task
    def generate_params_from_execution_date_day():
        context = get_current_context()
        execution_date: DateTime = context['execution_date']
        logging.info(f'execution_date.day = {execution_date.day}')
        return map(lambda day: f'table_{day}', range(execution_date.day))

    @task
    def refresh_materialized_view(refreshed_tables):
        logging.info(f'>>> Refreshing materialized view for tables {", ".join(refreshed_tables)}')

    tables = refresh_table.expand(table_name=generate_params_from_execution_date_day())
    refresh_materialized_view(tables)


dag