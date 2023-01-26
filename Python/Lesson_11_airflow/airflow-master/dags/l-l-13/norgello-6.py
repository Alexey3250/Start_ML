from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
        'norgello-2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='first task in lesson 11',
        schedule_interval=timedelta(days=3650),
        start_date=datetime(2022, 10, 20),
        catchup=False
) as dag:
    for i in range(10):
        NUMBER = i
        m1 = BashOperator(
            task_id=f'bash_command{i}',
            bash_command="echo $NUMBER")