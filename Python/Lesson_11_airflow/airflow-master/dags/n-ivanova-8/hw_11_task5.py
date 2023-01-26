from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

ds_date = "{{ ds }}"


def print_task_num(task_number):
    print(f'task number is: {task_number}')

with DAG(
        'hw_5_DAG_nivanova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 18),
        catchup=False,
        tags=['hw'],
) as dag:


    # "Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать значение run_id".
    # Здесь ts и run_id - это шаблонные переменные (вспомните, как в лекции подставляли шаблонные переменные).
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """)

    bash_templated = BashOperator(
        task_id='bash_templated',
        depends_on_past=False,
        bash_command=templated_command,
    )