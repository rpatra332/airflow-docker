from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args={
    "owner": "rp"
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_postgres_operator_v2",
    start_date=datetime(2024, 4, 22, 2),
    schedule="@daily",
    catchup=False
) as dag:
    
    ### TABLE CREATION PHASE ###
    task1=PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost:5431",
        sql="""
                create table if not exists dag_runs(
                    dt date,
                    dag_id character varying,
                    primary key (dt, dag_id)
                );
            """
    )


    ### DATA INSERTION PHASE ###
    # Jinja Templating
    # For {{ ds }} and {{ dag.dag_id }}
    # REFERENCE: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    task2=PostgresOperator(
        task_id="insert_into_table",
        postgres_conn_id="postgres_localhost:5431",
        sql="""
                insert into 
                dag_runs(dt, dag_id)
                values
                ('{{ ds }}', '{{ dag.dag_id }}');
            """
    )


    ### TABLE DELETEION PHASE ###
    task3=PostgresOperator(
        task_id="drop_postgres_table",
        postgres_conn_id="postgres_localhost:5431",
        sql="""
                drop table if exists dag_runs;
            """
    )

    task1 >> task2 >> task3