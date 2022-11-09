def print_execution_date(**context):
    execution_date = context['execution_date']
    timeshift_date = context['ti'].xcom_pull(key=None, task_ids='timeshift_date')
    if timeshift_date:
        print('Got execution date {}'.format(timeshift_date))
    else:
        print('Got execution date {}'.format(execution_date))
