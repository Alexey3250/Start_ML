from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'kolomiets_11_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lerning DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 21),
        catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'e-kolomiets_10_echos_{i}',
            bash_command=f"echo{i}"
        )


    def task_num(task, **kwargs):
        print(f'task number is: {task}')


    for i in range(20):
        t2 = PythonOperator(
            task_id=f"task_number_{i}",
            python_callable=task_num,
            op_kwargs={'task': i}
        )

    t1 >> t2
