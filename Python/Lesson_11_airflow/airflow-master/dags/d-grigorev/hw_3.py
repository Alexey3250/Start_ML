from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'homework_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2022, 1, 1),
        tags=['hw_3_d-grigorev']
) as dag:
    tasks = []
    for i in range(10):
        t = BashOperator(
            task_id=f'task_{i}',
            bash_command=f'echo {i}',
            dag=dag
        )
        tasks.append(t)


    def print_ds(ds, **kwargs):
        print(ds)
        return 'printing ds'


    t2 = PythonOperator(
            task_id='printing_ds',
            dag=dag,
            python_callable=print_ds
    )

    tasks.append(t2)

t1 = tasks[0]
for t in tasks[1:]:
    t1 >> t
    t1 = t
