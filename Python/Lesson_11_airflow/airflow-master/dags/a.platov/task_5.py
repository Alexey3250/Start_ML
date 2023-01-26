from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task',
        start_date=datetime(2022, 7, 17),
        catchup=False,
        tags=['first_task'],
    ) as dag:
        
        for task_number in range(10):
            t_bash = BashOperator(
                    task_id='HW_5_'+str(task_number),  # id, будет отображаться в интерфейсе
                bash_command='echo $NUMBER',
                env={"NUMBER": str(task_number)},
                dag=dag,
            )
        
        t_bash
