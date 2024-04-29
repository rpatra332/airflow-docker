from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args = {
    "owner": "rp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id="dag_with_python_dependency_v1",
    default_args=default_args,
    start_date=datetime(2023, 4, 22, 2),
    schedule_interval="@daily",
    catchup=False
)
def hello_world():

    @task()
    def get_scikitlearn_version():
        import sklearn
        print("Scikit-Learn Version::", sklearn.__version__)
    
    get_scikitlearn_version()
    
greet_dag = hello_world()
