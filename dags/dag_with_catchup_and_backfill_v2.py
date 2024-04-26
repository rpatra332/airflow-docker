from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "rp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="dag_with_catchup_and_backfill_v2",
    default_args=default_args, 
    start_date=datetime(2024, 4, 22, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo This is just a echo of things to come."
    )

# DAG run using backfill 
# In airflow-scheduler-container-bash, need to run command for using the backfill feature.
# COMMAND: airflow dags backfill -s <START_DATE> -e <END_DATE> <DAG_ID>
# NOTE: all dates in above command are in yyyy-MM-dd format