from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id='trigger_all_trap',
    start_date=datetime(2020, 1, 3, 0, 0),
    schedule_interval='@hourly'
)

with dag:
    create_resource1 = DummyOperator(task_id='create_resource1')
    create_resource2 = DummyOperator(task_id='create_resource2')
    use_resources = DummyOperator(task_id='use_resources')
    # run dag & mark user_resources as failed
    clean_resources = DummyOperator(task_id='clean_resources', trigger_rule=TriggerRule.ALL_DONE)
    dag_state_setter = DummyOperator(task_id='dag_state_setter', trigger_rule=TriggerRule.ALL_SUCCESS)

    create_resource1 >> create_resource2 >> use_resources >> [dag_state_setter, clean_resources]
