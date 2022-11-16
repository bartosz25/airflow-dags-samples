#!/usr/bin/env bash
mkdir /tmp/spark_job_data
airflow db init
airflow users create --username "admin" --role "Admin" --password "admin" --email "empty" --firstname "admin" --lastname "admin"
airflow webserver & airflow scheduler & airflow triggerer