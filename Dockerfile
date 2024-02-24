FROM apache/airflow:latest

USER root


RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
RUN pip install --no-cache-dir apache-airflow-providers-dbt-cloud



