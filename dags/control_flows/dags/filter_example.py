import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='filter_example',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    max_active_runs=1
)

with dag:
    def filter_objects(**context):
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
        return list_objects_from_cloud_object_storage(execution_date.day)

    objects_filter = PythonOperator(
        task_id='objects_filter',
        python_callable=filter_objects,
        provide_context=True,

    )

    def submit_processed_files_to_spark_job(**context):
        files_to_process = context['ti'].xcom_pull(key=None, task_ids='objects_filter')
        print(f'Executing spark-submit with: {files_to_process}')

    spark_job_trigger = PythonOperator(
        task_id='spark_job_trigger',
        python_callable=submit_processed_files_to_spark_job,
        provide_context=True,
    )

    objects_filter >> spark_job_trigger
