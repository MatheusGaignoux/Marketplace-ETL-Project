from airflow.models import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from dateutils import relativedelta
# from airflow.operators.python import get_current_context

spark_conn_id = "spark_cluster_conn"
postgres_conn_id = "postgres_b2b_datamart_conn"

ensure_length = lambda x: str(x) if len(str(x)) % 2 == 0 else "0" + str(x)
start_year = 2017
date_diff = relativedelta(datetime.today() - relativedelta(months = 1), datetime(start_year, 1, 1))
n = date_diff.years * 12 + date_diff.months
date_range = []
tasks = []

for i in range(0, n + 1):
    date = datetime(start_year, 1, 1) + relativedelta(months = i)
    date_range.append(date)

default_args = {
    "start_date": datetime(2022, 10, 1)
}

with DAG("historical_load", schedule_interval = None, default_args = default_args, catchup = False) as dag:
    catalog_dump = SSHOperator(
        task_id = "catalog_dump",
        ssh_conn_id = "sshserver_conn",
        command = "python /workspace/data-generation/catalog_dump.py"
    ) 
    
    data_generation = SSHOperator(
        task_id = "data_generation",
        ssh_conn_id = "sshserver_conn",
        command = "python /workspace/data-generation/historical_data_generation.py"
    ) 

    transform = SparkSubmitOperator(
        task_id = "data_transform",
        application = "/workspace/data-transform-load/transform.py",
        conn_id = spark_conn_id,
        jars = "/jars/delta-core_2.12-1.0.0.jar,/jars/postgresql-42.5.0.jar",
        application_args = ["-d", "{{ds}}", "-hl", "1"]
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

    for d in date_range:
        month = ensure_length(d.month)
        year = ensure_length(d.year)
        remote_path = f"/b2b-platform/data/logs/year={year}/month={month}/file.log"
        target_path = f"/mnt/landing/weblogs/year={year}/month={month}/file.log"
        task_id = f"data_transfer_sftp_{year}{month}"
        data_transfer = SFTPOperator(
            task_id = task_id,
            ssh_conn_id = "sshserver_conn",
            local_filepath = target_path,
            remote_filepath = remote_path,
            operation = "get",
            create_intermediate_dirs = True
        )

        tasks.append(data_transfer)

    catalog_dump >> data_generation >> tasks >> transform >> drop_views >> load >> [order_devices_report, popular_products_report, last_year_orders_report]
