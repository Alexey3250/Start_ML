from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_task_number(task_number):
    print(f'task number is: {task_number}')


with DAG(
    'task_5_vepifanov',
    description='vepifanov, задание 11.5',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['vepifanov'],
) as dag:

    task = BashOperator(
        task_id=f'bash_template_task',
        bash_command=dedent(
            """
            {% for i in range(5) %}
                echo "{{ts}}"
                echo "{{run_id}}"
            {% endfor %}
            """)
    )
    task
