FROM bitnami/spark:3.2.2

USER root

RUN apt-get update && apt-get install -y \
    openssh-server \
    wget \
    && mkdir -p /run/sshd \
    && ssh-keygen -A \
    && echo 'root:rootpassword' | chpasswd \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && service ssh start

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P /opt/bitnami/spark/jars/

CMD /usr/sbin/sshd -D & /opt/bitnami/spark/sbin/start-master.sh

