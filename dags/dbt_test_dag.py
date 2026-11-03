from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt/torino_pulse"

default_args = {
    'owner': 'iman',
    'retries': 0,
}

# Runs the dbt data tests a few times a day instead of after every dbt_transform
# run. The tests scan the large staging views, so they are expensive; the models
# they check are rebuilt every 15 minutes by dbt_transform.
with DAG(
    dag_id='dbt_test',
    default_args=default_args,
    start_date=datetime(2026, 11, 4),
    schedule_interval='0 */6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['torino-pulse', 'dbt'],
) as dag:

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .',
    )
