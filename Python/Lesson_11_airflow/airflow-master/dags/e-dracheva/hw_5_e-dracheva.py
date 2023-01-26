from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(

    'HW_4_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    description='HW 4 EDracheva',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_4_e-dracheva'],
    ) as dag:
    
    temp = dedent("""
       {% for i in range(5) %}
           echo "{{ ts }}"
           
           echo "{{ run_id }}"
       {% endfor %}
        """)
    
    t1 = BashOperator(
        task_id="echo",
        bash_command=temp
        )
    t1