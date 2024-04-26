from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    "owner": "rp"
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_cron_expression_v1",
    start_date=datetime(2024, 4, 22, 2),
    schedule="@daily",
    catchup=False
) as dag:
    task1=BashOperator(
        task_id="task1",
        bash_command="echo dag with cron expression"
    )