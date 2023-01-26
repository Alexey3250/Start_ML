from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'HW_6_s-dzhumaliev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id=f"echo_{i}",
            env={"NUMBER": i},
            bash_command="echo $NUMBER"
        )
