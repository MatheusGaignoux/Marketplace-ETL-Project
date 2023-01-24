from airflow.models import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from datetime import datetime
# from airflow.operators.python import get_current_context

ensure_length = lambda x: str(x) if len(str(x)) % 2 == 0 else "0" + str(x)

default_args = {
    "start_date": datetime(2022, 10, 1)
}

# month = "{{execution_date.month}}"
month = ensure_length(datetime.today().strftime("%m"))
year = datetime.today().strftime("%Y")
remote_path = f"/b2b-platform/data/logs/year={year}/month={month}/file.log"
target_path = f"/mnt/landing/weblogs/year={year}/month={month}/file.log"

with DAG("data_transfer", schedule_interval = None, default_args = default_args, catchup = False) as dag:
    
    data_transfer = SFTPOperator(
        task_id = "logs_data_transfer_sftp",
        ssh_conn_id = "sshserver_conn",
        local_filepath = target_path,
        remote_filepath = remote_path,
        operation = "get",
        create_intermediate_dirs = True
    )
