# DAG example for output invalidation pattern
1. Run Docker images with `docker-compose up`
2. `docker ps` to get the ID of the Airflow's container
3. Go to the Airflow's container (`docker exec -ti c3d855e1dc39  bash`) and start `airflow scheduler`
4. Enable `init_test` DAG
5. Trigger `init_test` DAG with http://localhost:8081/admin/airflow/trigger?dag_id=init_test
6. Enable `output_invalidation_pattern` DAG
