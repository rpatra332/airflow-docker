from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args = {
    "owner": "rp"
}

@dag(
    dag_id="dag_with_python_dependency_v2",
    default_args=default_args,
    catchup=False
)
def hello_world():
    @task()
    def get_scikitlearn_version():
        import sklearn
        print("Scikit-Learn Version::", sklearn.__version__)
    
    @task()
    def get_matplotlib_version():
        import matplotlib
        print("Matplotlib Version::", matplotlib.__version__)

    @task()
    def get_pandas_version():
        import pandas
        print("Pandas::", pandas.__version__)
    
    get_scikitlearn_version()
    get_matplotlib_version()
    get_pandas_version()
    
greet_dag = hello_world()
