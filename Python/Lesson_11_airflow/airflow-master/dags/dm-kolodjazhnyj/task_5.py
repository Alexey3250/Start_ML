"""
Some doc
"""

from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator


with DAG(
    'task_5_dm-kolodjazhny',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},
    start_date=datetime(2022, 8, 9)
    ) as dag:

    templated_command = dedent("""
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    task = BashOperator
    """)    
    
    task = BashOperator(
        task_id = 'bash_prikol',
        depends_on_past=False,
        bash_command=templated_command
    )