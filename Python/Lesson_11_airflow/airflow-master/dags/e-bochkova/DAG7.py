from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent


with DAG(
    'hw_7_e-bochkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description = 'A simple homework DAG',
    # Как часто запускать DAG
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup = False,
    # теги, способ помечать даги
    tags = ['homework3'],
) as dag:

    def print_task_number(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(f"ts is: {ts}")
        print(f"run_id is: {run_id}")


    t1 = PythonOperator(
        task_id=f"task_0",
        python_callable=print_task_number,
        op_kwargs={'task_number': 0},
    )

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        t1 >> t2
        t1 = t2
