from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"


with DAG(
        's_pletnev_task_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_3_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 23),
        catchup=False,
        tags=['task_3'],
) as dag:
    for i in range(5):
        task_1 = BashOperator(
            task_id=f"echo_task_number_{i}",
            env={"NUMBER": i},
            bash_command="echo $NUMBER",
        )
        task_1.doc_md = dedent(f"""
                # bash env NUMBER
                **Doc `BashOperator` *echo_$NUMBER*.
            """)
