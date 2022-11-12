"""
Task group is an alternative for declaring a group of multiple tasks. It's an alternative to already deprecated SubDAGs.

The example uses the Task group to represent partitions merge with the new data loaded to one staging table.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'start_date': datetime(2022, 1, 1)
}

@dag('tables_update', schedule_interval='@daily', default_args=default_args, max_active_runs=1, catchup=False)
def create_dag_with_taskflow_api():

    @task
    def data_availability_checker():
        return True

    @task
    def staging_data_loader():
        return 'staging_table_xyz'

    @task
    def materialized_view_refresher():
        return 'materialized_view'

    with TaskGroup(group_id='partitions_cleaners') as partitions_cleaners:
        months_to_clean = range(12)
        sub_task_groups = []
        for month_to_clean in months_to_clean:
            with TaskGroup(group_id=f'month_{month_to_clean}', parent_group=partitions_cleaners) as month_cleaners:
                partition_1_cleaner = EmptyOperator(task_id=f'merge_{month_to_clean}_partition_1')
                partition_2_cleaner = EmptyOperator(task_id=f'merge_{month_to_clean}_partition_2')
            sub_task_groups.append([partition_1_cleaner, partition_2_cleaner])

        sub_task_groups

    data_availability_checker() >> partitions_cleaners >> materialized_view_refresher()


dag = create_dag_with_taskflow_api()