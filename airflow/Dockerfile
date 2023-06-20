FROM apache/airflow:2.6.1
COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt
USER root
RUN apt-get update
RUN apt-get install software-properties-common -y --no-install-recommends
RUN apt-get install default-jdk -y --no-install-recommends
