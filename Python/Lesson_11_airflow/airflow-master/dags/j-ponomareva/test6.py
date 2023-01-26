from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"

with DAG(
    'hw_3_j_ponomareva_01',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG3_jp',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['j_pon_03'],
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f"echo_task_number_{i}",
                bash_command="echo $NUMBER"
            )
        else:
            t2 = PythonOperator(
                task_id=f"print_task_number_{i}",
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

            t1 >> t2
