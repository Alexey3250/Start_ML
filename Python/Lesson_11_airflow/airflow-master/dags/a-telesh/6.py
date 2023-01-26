from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_task(task_number, ts, run_id):
    print(f'task number is: {task_number}')
    print(f'ts is: {ts}')
    print(f'run_id is: {run_id}')


with DAG(
    'hw_6_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
) as dag:
    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id=f't_bash{i}',
                env={'NUMBER': str(i)},
                bash_command='echo $NUMBER',
                )
        else:
            task = PythonOperator(
                task_id=f't_python{i}',
                python_callable=print_task,
                op_kwargs={'task_number': i}
            )
