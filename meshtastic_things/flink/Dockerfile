FROM flink:1.19.2-scala_2.12-java17

USER root

RUN apt-get update -y && \
    apt-get install -y \
      default-jdk \
      python3 \
      python3-dev \
      python3-pip \
      python3-venv \
      && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /build
COPY . .
RUN pip install -r flink/requirements-flink.txt
RUN pip install .

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar

RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

WORKDIR /opt/flink/usrlib
COPY flink/decode_job.py flink/calibration_job.py flink/pattern_detection_job.py .

USER flink
