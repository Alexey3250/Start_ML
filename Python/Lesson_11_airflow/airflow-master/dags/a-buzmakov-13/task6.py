from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    'a-buzmakov-13_task_6',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_6'],
) as dag:
    for i in range(10):
        a=BashOperator(
            task_id="a"+str(i),            
            bash_command="echo $NUMBER",
            env={"NUMBER":str(i)}
            )

