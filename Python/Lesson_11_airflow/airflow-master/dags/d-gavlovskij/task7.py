from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task7_d-gavlovskij',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='somedag',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 10, 31),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['gavlique']
) as dag:

    def print_number(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'print_task_num_{i}',
                bash_command=f'echo {i}',
                dag=dag
            )
        else:
            t2 = PythonOperator(
                task_id=f'print_task_num_{i}',
                python_callable=print_number,
                dag=dag,
                op_kwargs={'task_number': i}
            )

    t1 >> t2
