from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
        'hw_6_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_6_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['hw_6'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_{i}',
            env={'NUMBER': i},
            bash_command=f'echo $NUMBER'
        )

    t1
