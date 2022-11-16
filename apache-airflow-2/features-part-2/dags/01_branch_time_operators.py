"""
The DAG shows new branch time operators. It represents a data ingestion pipeline with 2 different processing logics.
"""
import logging
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.datetime import BranchDateTimeOperator

with DAG(dag_id="raw_data_ingest", schedule='@daily', start_date=datetime(2022, 11, 4)) as dag:

    @task
    def v1_process_raw_data_1() -> str:
        return 'raw_data_1'

    @task
    def v1_process_raw_data_2() -> str:
        return 'raw_data_2'

    @task
    def v1_combine_raw_datas(row_data_1_location: str, row_data_2_location: str):
        print('Combining raw datas')

    @task
    def v2_process_raw_data_1_and_2():
        print('Processing raw data from both sources')

    @task
    def refresh_materialized_view(refreshed_tables):
        logging.info(f'>>> Refreshing materialized view for tables {", ".join(refreshed_tables)}')

    # After November, the 7th, we're switching to the V2
    processing_version_dispatcher = BranchDateTimeOperator(
        task_id='processing_version_dispatcher',
        follow_task_ids_if_true=['v1_process_raw_data_1', 'v1_process_raw_data_2'],
        follow_task_ids_if_false=['v2_process_raw_data_1_and_2'],
        target_lower=pendulum.datetime(2020, 10, 10, 15),
        target_upper=pendulum.datetime(2022, 11, 7),
        use_task_logical_date=True
    )

    raw_data_1 = v1_process_raw_data_1()
    raw_data_2 = v1_process_raw_data_2()
    combined_v1 = v1_combine_raw_datas(raw_data_1, raw_data_2)

    combined_v2 = v2_process_raw_data_1_and_2()

    processing_version_dispatcher >> [raw_data_1, raw_data_2, combined_v2]

dag