from openjdk:8-jre-slim

arg airflow_version=2.3.1 
arg spark_version=3.1.3
env AIRFLOW_HOME=/airflow

run mkdir -p ${AIRFLOW_HOME}/dags

copy airflow/config ${AIRFLOW_HOME}
copy airflow/init.sh ${AIRFLOW_HOME}/init.sh
run chmod a+x ${AIRFLOW_HOME}/init.sh

run apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update -y && \
    apt-get install -y python3-pip && \
    apt-get install -y postgresql-client && \
    pip3 install apache-airflow==${airflow_version} \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.1/constraints-3.7.txt" && \
    pip3 install "apache-airflow[postgres]" && \
    pip3 install pyspark==${spark_version} && \
    pip3 install delta-spark==1.0.0 && \
    pip3 install apache-airflow-providers-apache-spark && \
    pip3 install apache-airflow-providers-ssh && \
    pip3 install apache-airflow-providers-sftp && \
    pip3 install dateutils
    
workdir ${AIRFLOW_HOME}

expose  8080

cmd /airflow/init.sh
