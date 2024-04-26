from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    "owner": "rp"
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_cron_expression_v3",
    start_date=datetime(2024, 4, 22, 2),
    schedule="0 3 * * TUE", # At 03:00 AM, only on Tuesday
    catchup=False
) as dag:
    task1=BashOperator(
        task_id="task1",
        bash_command="echo dag with cron expression"
    )