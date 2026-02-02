FROM flink:1.18-java11

# Install Python 3, pip, and curl
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Upgrade pip and install PyFlink
COPY requirements.txt /opt/flink/
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Download Flink Kafka Connector and JSON format
RUN wget -P /opt/flink/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar

WORKDIR /opt/flink/usrlib
COPY processor.py .