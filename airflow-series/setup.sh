docker-compose -f docker-compose-LocalExecutor.yml up -d
docker-compose -f docker-compose-LocalExecutor.yml exec webserver airflow initdb
docker-compose -f docker-compose-LocalExecutor.yml exec webserver airflow connections -a --conn_id postgres_conn --conn_type 'Postgres' --conn_login 'airflow' --conn_password 'airflow' --conn_port 5432 --conn_host 'postgres'