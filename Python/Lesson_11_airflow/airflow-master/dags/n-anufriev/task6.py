from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
        'hw_6_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 31),
        catchup=False,
        tags=['hw_6_n-anufriev']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'task_number_is_{i}',
            bash_command="echo $NUMBER",
            env={'NUMBER': i})
