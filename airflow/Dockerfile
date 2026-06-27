FROM apache/airflow:3.2.2

USER root 

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt .

USER airflow

RUN pip install --no-cache-dir  -r requirements.txt

