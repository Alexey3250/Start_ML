from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    "hw_5_v-gorohov_cool_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:

    command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
        """
    )

    bash_operator = BashOperator(
        task_id=f"bash_command_templated",
        bash_command=command,
        dag=dag
        )