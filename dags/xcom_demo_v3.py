from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance


default_args = {
    "owner": "rp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


def greet(ti: TaskInstance):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello World, I am {first_name} {last_name} of age {age}.")


def get_name(ti: TaskInstance):
    ti.xcom_push(key="first_name", value="Jerry")
    ti.xcom_push(key="last_name", value="Fisher")


def get_age(ti: TaskInstance):
    ti.xcom_push(key="age", value=19)


with DAG(
    default_args=default_args,
    dag_id="python_operator_xcom_v03",
    start_date=datetime(2023, 4, 22, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet
    )
    
    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )

    task2 >> [task1, task3]