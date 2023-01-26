from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'j-jancharskaja_5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My fourth DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['fourth']
) as dag:

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo '{{ ts }}'
        echo '{{ run_id }}'
    {% endfor %}
    """
    )

    t1 = BashOperator(
        task_id='templated',
        bash_command = templated_command,       
    )

    t1