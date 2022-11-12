#!/usr/bin/env bash
airflow db init
airflow users create --username "admin" --role "Admin" --password "admin" --email "empty" --firstname "admin" --lastname "admin"
airflow webserver & airflow scheduler
