FROM apache/airflow:3.0.0
USER airflow
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY /dags /opt/airflow/dags/