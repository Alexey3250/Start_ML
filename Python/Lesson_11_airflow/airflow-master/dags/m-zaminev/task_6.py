from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

with DAG(
    'm-zaminev_task_7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='m-zaminev_task_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=['m-zaminev_task_7']

) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = 't_1_echo_' + str(i),
            bash_command = f'echo $NUMBER',
            env = {"NUMBER": str(i)}
        )

    t1
