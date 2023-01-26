from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def print_task_number(ts, run_id, **kwargs):
    print(kwargs)
    print(ts)
    print(run_id)
    return None

with DAG(
    'n-ubozhenko-10-lesson-11-step-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 17),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='task_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i},
        )

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_' + str(i+10),
            python_callable=print_task_number,
            op_kwargs={'task_number': i+10},
        )

t1 >> t2






