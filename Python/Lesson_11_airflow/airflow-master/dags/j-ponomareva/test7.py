from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_7_j_ponomareva_01',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG7_jp',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['j_pon_07'],
) as dag:
    def print_task_number(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f"task number is: {task_number}")

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f"echo_task_number_{i}",
                bash_command=f"echo {i}"
            )
        else:
            t2 = PythonOperator(
                task_id=f"print_task_number_{i}",
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

            t1 >> t2

