from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash import BashOperator


with DAG(
    '11_6_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],
) as dag:
    for i in range(1, 11):
        t1 = BashOperator(
            task_id='task_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )

