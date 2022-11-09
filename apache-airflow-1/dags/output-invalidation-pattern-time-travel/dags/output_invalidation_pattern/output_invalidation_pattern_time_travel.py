from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum

dag = DAG(
    dag_id='output_invalidation_pattern_time_travel',
    schedule_interval=timedelta(hours=1),
    start_date=pendulum.datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    max_active_runs=1
)

with dag:
    def generate_version_name(**context):
        execution_date = context['execution_date']
        delta_lake_version_to_restore = (execution_date - dag.start_date).total_seconds() / dag.schedule_interval.total_seconds()
        return delta_lake_version_to_restore

    version_getter = PythonOperator(
        task_id='version_getter',
        python_callable=generate_version_name,
        provide_context=True
    )

    def submit_spark_job(**context):
        # It's only a dummy implementation to show how to retrieve
        # the version.
        version_to_pass = context['ti'].xcom_pull(key=None, task_ids='version_getter')
        print('Got version {}'.format(version_to_pass))
        return 'spark-submit .... {}'.format(version_to_pass)

    spark_job_submitter = PythonOperator(
        task_id='spark_job_submitter',
        python_callable=submit_spark_job,
        provide_context=True
    )

    version_getter >> spark_job_submitter