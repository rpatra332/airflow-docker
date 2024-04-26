from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args = {
    "owner": "rp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id="dag_with_taskflow_api_v1",
    default_args=default_args,
    start_date=datetime(2023, 4, 22, 2),
    schedule_interval="@daily",
    catchup=False
)
def hello_world():

    @task()
    def get_name():
        return "Jerry"
    
    @task()
    def get_age():
        return 19

    @task()
    def greet(name, age):
        print(f"Hello World, I am {name} of age {age}.")


    name = get_name()
    age = get_age()
    greet(name=name, age=age)

greet_dag = hello_world()
