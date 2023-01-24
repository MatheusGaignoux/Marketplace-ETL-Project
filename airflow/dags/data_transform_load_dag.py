from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

spark_conn_id = "spark_cluster_conn"
postgres_conn_id = "postgres_b2b_datamart_conn"
default_args = {
    "start_date": datetime(2022, 10, 1)
}

with DAG("data_transform_load", schedule_interval = None,default_args = default_args, catchup = False) as dag:
    transform = SparkSubmitOperator(
        task_id = "data_transform",
        application = "/workspace/data-transform-load/transform.py",
        conn_id = spark_conn_id,
        jars = "/jars/delta-core_2.12-1.0.0.jar,/jars/postgresql-42.5.0.jar",
        application_args = ["-d", "{{ds}}", "-hl", "0"]
    )

    drop_views = PostgresOperator(
        task_id = "drop_views",
        postgres_conn_id = postgres_conn_id,
        sql = "sql/drop_views.sql"
    )

    load = SparkSubmitOperator(
        task_id = "data_load",
        application = "/workspace/data-transform-load/load.py",
        conn_id = spark_conn_id,
        jars = "/jars/delta-core_2.12-1.0.0.jar,/jars/postgresql-42.5.0.jar"
    )

    order_devices_report = PostgresOperator(
        task_id = "orders_devices_report",
        postgres_conn_id = postgres_conn_id,
        sql = "sql/order_devices_report.sql"
    )

    popular_products_report = PostgresOperator(
        task_id = "popular_products_report",
        postgres_conn_id = postgres_conn_id,
        sql = "sql/popular_products_report.sql"
    )

    last_year_orders_report = PostgresOperator(
        task_id = "last_year_orders_report",
        postgres_conn_id = postgres_conn_id,
        sql = "sql/last_year_orders_report.sql"
    )

    transform >> drop_views >> load >> [order_devices_report, popular_products_report, last_year_orders_report]
