from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "rp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="dag_with_catchup_and_backfill_v1",
    default_args=default_args, 
    start_date=datetime(2024, 4, 22, 2),
    schedule_interval="@daily",
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo This is just a echo of things to come."
    )
