from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from textwrap import dedent


with DAG (
    'task_5_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_5'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """)
    
    task = BashOperator(
        task_id='print_ts_run_id',
        bash_command=templated_command,
    )
    
    
