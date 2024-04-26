from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "rp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

def greet(name, age):
    print(f"Hello World, I am {name} of age {age}.")

with DAG(
    default_args=default_args,
    dag_id="create_dag_with_python_operator_v02",
    description="our_first_dag using a python operator",
    start_date=datetime(2023, 4, 22, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs= {
            "name": "Rohit",
            "age": "22"
        }
    )