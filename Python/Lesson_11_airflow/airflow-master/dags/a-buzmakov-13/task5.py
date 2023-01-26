from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'a-buzmakov-13_task_5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_5'],
) as dag:
    templated_code = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """)
    a = BashOperator(
        task_id='command',
        bash_command=templated_code)
 
