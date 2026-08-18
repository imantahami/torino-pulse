from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt/torino_pulse"

default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_transform',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['torino-pulse', 'dbt'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .',
    )

    dbt_run >> dbt_test