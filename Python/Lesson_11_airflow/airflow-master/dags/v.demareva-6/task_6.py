from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

with DAG \
            (
            "task_6_v_demareva",
            default_args={
                'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            },
            description="DAG for task #6",
            schedule_interval=timedelta(days=1),
            start_date=datetime(2022, 8, 21),
            catchup=False,
            tags=["task_6"]
        ) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"t6_bash_{i}",
            bash_command=f"echo $NUMBER",
            env={'NUMBER': i}
        )

    t1



