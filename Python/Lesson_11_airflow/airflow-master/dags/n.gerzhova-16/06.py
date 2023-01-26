from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'NG_sixth',
        default_args={
            'depends_on_past': False,
            'email': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='DAG for Task 06',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 23),
) as dag:

    def print_context(ts, run_id, task_number):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(11, 31, 1):
        t_11_30 = PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )
