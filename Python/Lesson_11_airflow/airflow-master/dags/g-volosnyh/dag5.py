from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'dag5_g-volosnyh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command="echo $NUMBER",
            env={"NUMBER": i},
        )

