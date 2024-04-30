import csv
import logging
from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "rp",
}


def postgres_to_s3():
    # step 1: query date from postgresql and save into text files
    hook = PostgresHook(postgres_conn_id="postgres_localhost:5431")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= '20220501'")
    with open("/opt/airflow/staging/get_orders.txt","w") as f:
        csv_writer = csv.writer(f)  
        csv_writer.writerow([ i[0] for i in cursor.description ])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved order data in textfile get_orders.txt")
    # step 2: upload the text files to S3
    
    

with DAG(
    dag_id="dag_with_postgres_hook_v1",
    start_date= datetime(2024, 4, 30),
    default_args=default_args,
    catchup=False
) as dag:
    task1=PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3,
    )