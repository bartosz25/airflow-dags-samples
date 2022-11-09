import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='foreach_example',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    max_active_runs=1
)

with dag:
    def trigger_job_for_filtered_objects(**context):
        def list_objects_from_cloud_object_storage(day):
            # Simulates objects retrieval from an object storage
            # We suppose here that the operation will list at most 3 objects
            # so it's safe to keep them in an XCom variable
            objects = {
                0: ['item_1.csv', 'item_2.csv'],
                1: ['item_1.csv', 'item_2.csv', 'item_3.csv']
            }
            return objects[day % 2]
        execution_date = context['execution_date']
        filtered_objects = list_objects_from_cloud_object_storage(execution_date.day)
        # Different than the filter_example since it doesn't include this extra filtering separate step
        # Hence, do not store the XCom
        print(f'Submitting a job for {filtered_objects}')
        return 'some_job_id'

    job_trigger = PythonOperator(
        task_id='job_trigger',
        python_callable=trigger_job_for_filtered_objects,
        provide_context=True,
    )

    job_completion_listener = DummyOperator(
        task_id='job_completion_listener'
    )

    job_trigger >> job_completion_listener
