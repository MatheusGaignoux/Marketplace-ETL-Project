from openjdk:8-jre-slim

run mkdir /workspace && \
    mkdir -p /mnt/data && \
    mkdir /jars && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update -y && \
    apt-get install -y python3-pip

copy requirements.txt /workspace/requirements.txt
copy jars /jars

workdir /workspace

run pip3 install -r requirements.txt && \
    pip3 install pyspark==3.1.3 && \
    pip3 install delta-spark==1.0.0 && \
    pip3 install sparksql-magic

volume /mnt
volume /workspace
volume /jars

expose 8888

cmd jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=