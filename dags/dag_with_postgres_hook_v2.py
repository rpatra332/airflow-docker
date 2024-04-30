import csv
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "rp",
}


def postgres_to_s3(ds_nodash, next_ds_nodash):
    # step 1: query date from postgresql and save into text files
    # dynamic sql queries and file names with airflow macros
    hook = PostgresHook(postgres_conn_id="postgres_localhost:5431")
    conn = hook.get_conn()
    cursor = conn.cursor()
    print("ds_nodash:",ds_nodash)
    print("next_ds_nodash", next_ds_nodash)
    cursor.execute("select * from orders where date >= %s and date <= %s;",(ds_nodash, next_ds_nodash))
    with open(f"/opt/airflow/staging/get_orders_{ds_nodash}.txt","w") as f:
        csv_writer = csv.writer(f)  
        csv_writer.writerow([ i[0] for i in cursor.description ])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()
    logging.info(f"Saved order data in textfile get_orders_{ds_nodash}.txt")
    # step 2: upload the text files to S3
    
    

with DAG(
    dag_id="dag_with_postgres_hook_v2",
    start_date= datetime(2022, 4, 10),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:
    task1=PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3,
    )