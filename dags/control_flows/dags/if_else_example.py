import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

dag = DAG(
    dag_id='if_else_example',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    max_active_runs=1
)

with dag:
    def check_execution_day(**context):
        execution_date = context['execution_date']
        return 'foo_printer' if execution_date.day % 2 == 0 else 'bar_printer'

    if_operator = BranchPythonOperator(
        task_id='if_operator',
        python_callable=check_execution_day,
        provide_context=True
    )

    def print_hello(**context):
        print(f"{context['word']}")

    foo_printer = PythonOperator(
        task_id='foo_printer',
        python_callable=print_hello,
        provide_context=True,
        op_kwargs={'word': 'foo'}
    )
    bar_printer = PythonOperator(
        task_id='bar_printer',
        python_callable=print_hello,
        provide_context=True,
        op_kwargs={'word': 'bar'}
    )

    if_operator >> [foo_printer, bar_printer]