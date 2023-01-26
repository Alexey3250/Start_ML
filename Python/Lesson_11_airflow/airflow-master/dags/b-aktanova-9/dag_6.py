from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_5_aktanova_b',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lesson 11 home work 1",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id=f"print_{i}",
            bash_command=f"echo $NUMBER",
            env={"NUMBER": str(i)}
        )