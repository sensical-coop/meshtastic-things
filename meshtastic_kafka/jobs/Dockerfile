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

COPY requirements-flink.txt requirements.txt
RUN pip install -r requirements.txt

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar

# RUN curl -L -o /opt/flink/lib/flink-connector-kafka.jar \
#  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar

WORKDIR /opt/flink/usrlib
COPY . .
USER flink
