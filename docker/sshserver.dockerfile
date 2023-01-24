from ubuntu:latest

run mkdir -p /workspace/data && \
    mkdir -p /b2b-platform && \
    apt update && \ 
    apt install openssh-server sudo -y && \
    apt-get -y update && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get -y update && \
    apt-get install -y python3-pip && \
    pip3 install psycopg2-binary==2.9.4 && \
    pip3 install dateutils && \
    pip3 install fake-useragent

run chmod -R 777 /b2b-platform
run useradd -rm -d /home/ubuntu -s /bin/bash -g root -G sudo -u 1000 docker 
run  echo 'docker:docker' | chpasswd
run service ssh start

copy workspace/data-generation /workspace/data-generation
workdir /workspace
volume /workspace

expose 22

CMD ["/usr/sbin/sshd","-D"]