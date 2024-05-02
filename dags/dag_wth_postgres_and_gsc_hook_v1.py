import csv
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
    "owner": "rp",
}


def postgres_to_gcs(ds_nodash, next_ds_nodash):
    # step 1: query date from postgresql and save into text files
    # dynamic sql queries and file names with airflow macros
    hook = PostgresHook(postgres_conn_id="postgres_localhost:5431")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date >= %s and date <= %s;",(ds_nodash, next_ds_nodash))
    with open(f"/opt/airflow/staging/get_orders_{ds_nodash}.txt","w") as f:
        csv_writer = csv.writer(f)  
        csv_writer.writerow([ i[0] for i in cursor.description ])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info(f"Saved order data in textfile get_orders_{ds_nodash}.txt")
    

with DAG(
    dag_id="dag_with_postgres_and_gcs_v1",
    start_date= datetime(2022, 4, 10),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:
    task1=PythonOperator(
        task_id="postgres_to_local",
        python_callable=postgres_to_gcs,
    )

    # step 2: upload the text files to google cloud storage
    task2=LocalFilesystemToGCSOperator(
        task_id="local_to_gcs",
        gcp_conn_id="google_cloud_conn",
        bucket="airflow-storage-etl-bucket",
        src="/opt/airflow/staging/get_orders_{{ds_nodash}}.txt",
        dst="orders/{{ds_nodash}}.txt"
    )

    task1>>task2
