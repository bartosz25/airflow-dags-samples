# DAG example for output invalidation pattern with Delta Lake time-travel feature

This repo contains only Apache Airflow DAG example, with an Apache Spark trigger example operator. 
The goal is to give you an idea how to resolve a version from Apache Airflow and handle the reprocessing use case.

1. Run Docker images with `docker-compose up`
2. Enable `output_invalidation_pattern_time_travel` DAG and observe what happens in the logs.
