from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "rp",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="our_first_dag_v2",
    default_args=default_args,
    description="This is our first dag that we write",
    start_date=datetime(2024, 4, 22, 2),
    schedule_interval="@daily"
) as dag:
    task1= BashOperator(
        task_id="first_task",
        bash_command="echo hello world, this is the first task!",
    )

    task2= BashOperator(
        task_id="second_task",
        bash_command="echo how you doing",
    )

    task1.set_downstream(task2)
    task2.set_upstream(task1)