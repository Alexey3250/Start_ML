from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent
from datetime import datetime, timedelta

with DAG(
    'hm_6_e-poltorakova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['e-poltorakova'],
) as dag:

    for i in range(10):
        t = BashOperator(
            task_id="print" + f"{i}",
            bash_command=f"echo $NUMBER",
            env={"NUMBER": str(i)}
        )
