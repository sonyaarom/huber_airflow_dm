FROM apache/airflow:2.7.1-python3.9

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt
