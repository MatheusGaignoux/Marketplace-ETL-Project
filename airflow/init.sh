#!/bin/bash

# test_case=$(pg_isalive -h postgres-airflow -p 5432)
# 
# while [ "${test_case}" != "postgres-airflow:5432 - accepting connections"] 
# do {
#     test_case=$(pg_isalive -h postgres-airflow -p 5432)
#     sleep 1m
# }
# done

PGPASSWORD=docker psql \
    -U docker airflow \
    -h postgres-airflow \
    -p 5432 \
    -c "select count(tablename) from pg_catalog.pg_tables where tableowner = 'airflow_user' and schemaname not in ('pg_catalog', 'information_schema')" > count.file

number_of_tables=$(sed -n 3p count.file | tr -d ' ')

if [ ${number_of_tables} -ge 1 ]
then {
    echo "Metastore is already defined. Passing."
}
else {
    echo "First execution. Initiating the metastore DB, creating an admin user and a connection."
    airflow db init
    
    airflow users create \
    -u docker \
    -p docker \
    -f docker -l aiflow -r Admin -e docker_airflow@airflow.com

    airflow connections add "spark_cluster_conn" \
    --conn-uri "spark://spark-master:7077"

    airflow connections add "sshserver_conn" \
    --conn-uri "ssh://docker:docker@sshserver:22"

    airflow connections add "postgres_b2b_datamart_conn" \
    --conn-uri "postgresql://docker:docker@postgres-datamart:5432/datamart"
}
fi
