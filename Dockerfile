FROM apache/airflow:2.6.0-python3.10
USER root
RUN apt-get update && apt-get install -y default-jdk netcat-openbsd && apt-get clean
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0 kafka-python
ENTRYPOINT ["/entrypoint.sh"]