from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


def print_task(task_number):
    print(f'task number is: {task_number}')


with DAG(
    'hw_4_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )

    task = BashOperator(
        task_id='template_task',
        bash_command=templated_command
    )

    task
