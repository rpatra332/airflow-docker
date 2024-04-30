from airflow.decorators import task, dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "rp"
}

@dag(
    dag_id="dag_with_minios3_v1",
    default_args=default_args,
    catchup=False,
)
def minios3_etl():
    # poking to check if the file`data.csv` exist in the S3_Bucket
    # it will check if the file exists or not 
    task1 = S3KeySensor(
        task_id = "sensor_minio_s3",
        bucket_name="airflow",
        bucket_key="data.csv",
        aws_conn_id="minio_connection"
    )