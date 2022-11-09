from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.PostgresCopyOperator import PostgresCopyOperator
from operators.PostgresViewManagerOperator import PostgresViewManagerOperator

dag = DAG(
    dag_id='output_invalidation_pattern',
    schedule_interval=timedelta(hours=1),
    default_args={'start_date': datetime(2019, 9, 30, 0)},
    catchup=True,
    max_active_runs=1
)

with dag:
    schema = 'output_invalidation_pattern'
    view_name = 'output_invalidation_pattern_view'

    def generate_weekly_table_name(**context):
        execution_date = context['execution_date']
        return 'week_{week_number}'.format(week_number=execution_date.week_of_year)

    wait_for_data_to_add = FileSensor(
        fs_conn_id='fs_default',
        task_id='wait_for_data_to_add',
        poke_interval=20,
        filepath='/tmp/output_invalidation_pattern/{{ts_nodash}}.csv'
    )

    generate_table_name = PythonOperator(
        task_id='generate_table_name',
        provide_context=True,
        python_callable=generate_weekly_table_name
    )

    def retrieve_path_for_table_creation(**context):
        execution_date = context['execution_date']
        is_monday_midnight = execution_date.day_of_week == 1 and execution_date.hour == 0
        return 'remove_table_from_view' if is_monday_midnight else "dummy_task"

    check_if_monday_midnight = BranchPythonOperator(
        task_id='check_if_monday_midnight',
        provide_context=True,
        python_callable=retrieve_path_for_table_creation
    )

    dummy_task = DummyOperator(
        task_id='dummy_task'
    )

    remove_table_from_view = PostgresViewManagerOperator(
        task_id='remove_table_from_view',
        postgres_conn_id='docker_postgresql',
        database=schema,
        view_name=view_name,
        params={'schema': schema},
        sql='create_view_without_table.sql'
    )

    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id='docker_postgresql',
        database=schema,
        sql="DROP TABLE IF EXISTS {{ti.xcom_pull(task_ids='generate_table_name')}}"
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='docker_postgresql',
        database=schema,
        sql='create_table.sql'
    )

    add_table_to_view = PostgresViewManagerOperator(
        task_id='add_table_to_view',
        postgres_conn_id='docker_postgresql',
        database=schema,
        view_name=view_name,
        params={'schema': schema},
        sql='create_view_with_table.sql'
    )

    add_data_to_table = PostgresCopyOperator(
        task_id='add_data_to_table',
        postgres_conn_id='docker_postgresql',
        database=schema,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        source_file='/tmp/output_invalidation_pattern/{{ ts_nodash }}.csv',
        target_table="{{ ti.xcom_pull(task_ids='generate_table_name') }}"
    )

    wait_for_data_to_add >> generate_table_name >> check_if_monday_midnight >> [dummy_task, remove_table_from_view]
    remove_table_from_view >> drop_table >> create_table >> add_table_to_view
    [add_table_to_view, dummy_task] >> add_data_to_table
