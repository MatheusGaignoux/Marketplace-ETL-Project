from airflow.models import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
# from airflow.operators.python import get_current_context

default_args = {
    "start_date": datetime(2022, 10, 1)
}

with DAG("data_generation", schedule_interval = None, default_args = default_args, catchup = False) as dag:
    data_generation = SSHOperator(
        task_id = "data_generation",
        ssh_conn_id = "sshserver_conn",
        command = "python /workspace/data-generation/runner.py -d {{ds}}"
    )

    [data_generation]
