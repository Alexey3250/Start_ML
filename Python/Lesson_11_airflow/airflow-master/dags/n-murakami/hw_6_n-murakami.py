from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_iter(task_number, **kwargs):
    print(kwargs)
    return 'task number is: {task_number}'

with DAG(
    'hw_6_n-murakami',
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_6_n-murakami'],
) as dag:

    for i in range(10):
        t=BashOperator(
            task_id='bash_'+str(i+1),
            env={"NUMBER": str(i+1)},
            bash_command="echo $NUMBER"
        )

