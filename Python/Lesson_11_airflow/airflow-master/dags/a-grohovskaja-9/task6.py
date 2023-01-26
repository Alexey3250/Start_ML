from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import os

with DAG(
    'a-grohovskaja-9_hw_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-grohovskaja-9_hw_5'],
) as dag:

    def print_task_number(ts, run_id, **kwargs):
        print(ts)
        print(run_id)


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                env={"NUMBER": i},
                task_id='bash_part' + str(i),
                bash_command="echo $NUMBER"
            )
        else:
            t2 = PythonOperator(
                task_id='python_part_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
    t1 >> t2