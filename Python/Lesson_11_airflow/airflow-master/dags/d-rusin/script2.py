from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

with DAG(
        'first_question',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Py-and-Bash-operators',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 22),
        catchup=False,
        tags=['task_2'],
) as dag:
    task1 = BashOperator(task_id="print_pwd", bash_command="pwd")


    def print_ds(ds):
        print(ds)
        print("!!!!cool!!!!")


    task2 = PythonOperator(task_id="print_ds", python_callable=print_ds)

    task1 >> task2
