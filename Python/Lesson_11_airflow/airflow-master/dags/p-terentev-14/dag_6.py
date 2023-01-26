from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        "hw_6_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 12),
        catchup=False,
        tags=['DAG_6']
) as dag:
    for i in range(10):
        bash_op = BashOperator(
            task_id='print_task' + str(i),
            bash_command=f'echo $NUMBER',
            env={'NUMBER': str(i)}
        )