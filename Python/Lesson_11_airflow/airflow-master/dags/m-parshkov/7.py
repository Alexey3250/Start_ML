from textwrap import dedent
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_number(task_number, ts, run_id, **kwargs):
    print(f"task number is: {task_number}")
    print(ts)
    print(run_id)


with DAG(
        '7_m_parshkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='my_7_m_parshkov',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 6, 17),
        catchup=True
) as dag:
      for i in range(20):
        t2 = PythonOperator(
            task_id=f'task_number_{i}',
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )




