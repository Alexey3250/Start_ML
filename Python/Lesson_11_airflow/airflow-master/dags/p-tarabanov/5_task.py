from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_5_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='4 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['Docs'],
) as dag:
    
    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )
    task1 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
    task1.doc_md = dedent(
    """ \
        # Task - templated_command
        It`s templated BashOperator.
        Writting is bash command - **echo**.
        For *AirFlow* - `BashOperator`.
    """
    )
    
        