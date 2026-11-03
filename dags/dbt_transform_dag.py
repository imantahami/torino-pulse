from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt/torino_pulse"

default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Rebuilds the models only. The tests live in the separate dbt_test DAG: a
# full run takes 8-14 minutes on the 4-core VM and the tests another 6, so
# running both every 15 minutes kept the server saturated and starved the
# dashboard. max_active_runs=1 stops runs from piling up on each other.
with DAG(
    dag_id='dbt_transform',
    default_args=default_args,
    start_date=datetime(2026, 8, 18),
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=['torino-pulse', 'dbt'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
    )
