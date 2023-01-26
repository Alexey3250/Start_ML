from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
with DAG(
    'm-zaminev_task_7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='m-zaminev_task_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=['m-zaminev_task_7']

) as dag:

    def print_task_number(ts, run_id, task_number):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)
        return "task number is printed"

    for i in range(20):
        t1 = PythonOperator(
            task_id='print_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )

    t1
