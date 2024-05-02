import csv
import logging
from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


default_args = {
    "owner": "rp",
}


@dag(
    dag_id="dag_with_postgres_and_gcs_v2",
    default_args=default_args,
    start_date= datetime(2022, 4, 10),
    schedule_interval="@daily",
    catchup=True
)
def orders_table_etl():
    @task()
    def postgres_to_gcs(ds_nodash, next_ds_nodash):
    # step 1: query date from postgresql and save into text files
    # dynamic sql queries and file names with airflow macros
        pg_hook = PostgresHook(postgres_conn_id="postgres_localhost:5431")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("select * from orders where date >= %s and date <= %s;",(ds_nodash, next_ds_nodash))
        with NamedTemporaryFile(mode="w", suffix=f"{ds_nodash}") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([ i[0] for i in cursor.description ])
            csv_writer.writerows(cursor)
            f.flush()
            cursor.close()
            conn.close()
            logging.info(f"Saved order data in textfile NamedTemporaryFile: '{f.name}'")

            # step 2: upload the text files to google cloud storage
            gcs_hook = GCSHook(gcp_conn_id="google_cloud_conn")
            gcs_hook.upload(
                bucket_name="airflow-storage-etl-bucket",
                filename=f.name,
                object_name=f"orders/{ds_nodash}.txt",
            )
            logging.info(f"Uploaded NamedTemporaryFile: '{f.name}' to the GCS Bucket as object: 'orders/{ds_nodash}.txt'")

    postgres_to_gcs()

orders_table_etl()