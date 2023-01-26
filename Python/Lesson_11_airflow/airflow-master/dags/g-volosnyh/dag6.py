from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'dag6_g-volosnyh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    def print_task_info(task_number, ts, run_id):
        print(f'ts is {ts}')
        print(f'run_id is {run_id}')
        return 0

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'task_info_{i}',
            python_callable=print_task_info,
            op_kwargs={'task_number': i}
        )


