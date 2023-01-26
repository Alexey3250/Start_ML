from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_7_j-filatov-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='This is a seventh exercise. PO and kwargs.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup=False,
    tags=['hw_7_j-filatov-13'],
) as dag:

    def print_context(ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(f"ts is: {ts}")
        print(f"run id is: {run_id}")
        return (f"I printed {task_number}, {ts}, {run_id} successfully!")

    for i in range(20):
        t2 = PythonOperator(
            task_id='po_print_task_number_ts_ds' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

    t2
