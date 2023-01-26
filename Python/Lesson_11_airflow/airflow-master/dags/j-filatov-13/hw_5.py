from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_5_j-filatov-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='This is a fifth excercise. BO templated.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup=False,
    tags=['hw_5_j-filatov-13'],
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='hw_5_j-filatov-13_templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1
