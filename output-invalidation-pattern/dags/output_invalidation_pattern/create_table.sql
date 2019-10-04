CREATE TABLE {{ ti.xcom_pull(task_ids='generate_table_name') }} (
    name VARCHAR(30) NOT NULL,
    age SMALLINT NOT NULL
)