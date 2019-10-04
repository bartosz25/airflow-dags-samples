SELECT table_name FROM information_schema.tables WHERE table_catalog='{{ params.schema }}'
AND table_schema = 'public'
AND table_type = 'BASE TABLE'
AND table_name != '{{ ti.xcom_pull(task_ids='generate_table_name') }}'