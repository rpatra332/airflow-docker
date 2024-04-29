# Extending the Airflow Image
# For docker-compose to work, Expected built image name: extended_airflow_image:latest
# Change accordingly

FROM apache/airflow:2.9.0
WORKDIR /
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./requirements.txt