from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def print_task(ts, run_id, **kwargs):
    print(f"task number is: {kwargs['task_number']}")
    print(f"ts parameter: {ts}")
    print(f"run_id parameter': {run_id}")

with DAG(

    'hw7_m-vasilevskij',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description = 'DAG for lesson 11',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 12, 14),
    catchup = False

) as dag:

    for i in range(0, 30):
        if i < 10:
            task = BashOperator(
                task_id = f"task_{i}",
                depends_on_past = False,
                retries = 3,
                bash_command = "echo $NUMBER",
                env = {'NUMBER': i}
            )
        else:
            task1 = PythonOperator(
                task_id = f"task_{i}",
                depends_on_past = False,
                retries = 3,
                python_callable = print_task,
                op_kwargs = {'task_number': i}
            )



    task >> task1





