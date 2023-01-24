from openjdk:8-jre-slim

arg spark_version=3.1.3
arg hadoop_version=3.2

run mkdir /workspace && \
    mkdir -p /mnt/data && \
    mkdir /jars && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

env SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
env SPARK_MASTER_HOST spark-master
env SPARK_MASTER_PORT 7077
env PYSPARK_PYTHON python3
env spark_master_web_ui 8080
env spark_worker_web_ui 8081

workdir ${SPARK_HOME}

expose ${SPARK_MASTER_PORT} ${spark_master_web_ui} ${spark_worker_web_ui}

cmd bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out
