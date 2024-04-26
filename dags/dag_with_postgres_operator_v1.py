from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args={
    "owner": "rp"
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_postgres_operator_v1",
    start_date=datetime(2024, 4, 22, 2),
    schedule="@daily",
    catchup=False
) as dag:
    task1=PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost:5431",
        sql="""
                create table if not exists dag_runs(
                    dt date,
                    dag_id character varying,
                    primary key (dt, dag_id)
                );
            """
    )