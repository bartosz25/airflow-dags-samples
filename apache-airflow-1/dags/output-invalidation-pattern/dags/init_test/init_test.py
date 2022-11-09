import csv
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='init_test',
    schedule_interval=None,
    default_args={'start_date': datetime(2019, 9, 30, 0)}
)


with dag:
    def generate_sample_files():
        os.makedirs('/tmp/output_invalidation_pattern', exist_ok=True)
        files_to_generate = ['/tmp/output_invalidation_pattern/20190930T000000.csv',
                             '/tmp/output_invalidation_pattern/20190930T010000.csv',
                             '/tmp/output_invalidation_pattern/20190930T020000.csv']
        for file in files_to_generate:
            with open(file, mode='w') as employee_file:
                employee_writer = csv.writer(employee_file, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

                employee_writer.writerow(['first name', '39'])
                employee_writer.writerow(['other name', '40'])

    add_connection = BashOperator(
        task_id='add_connection',
        bash_command='airflow connections --add --conn_id docker_postgresql --conn_host 191.18.0.21 '
                     '--conn_type postgres --conn_login airflow_test --conn_password airflow_test --conn_port 5432',
    )
    generate_sample_data = PythonOperator(
        task_id='generate_simple_data',
        python_callable=generate_sample_files
    )

    add_connection >> generate_sample_data
