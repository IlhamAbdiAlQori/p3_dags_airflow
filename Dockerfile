FROM apache/airflow:2.9.2
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Use the official Airflow image
FROM apache/airflow:2.5.1

# Install PySpark
USER root
RUN pip install pyspark
USER airflow
