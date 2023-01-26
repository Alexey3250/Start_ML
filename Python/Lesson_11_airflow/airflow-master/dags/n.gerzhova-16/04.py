from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


with DAG(
    'NG_fourth',
    default_args={
        'depends_on_past': False,
        'email': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
   
    description='DAG for Task 04',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 23),
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
        task_id='04_template', 
        bash_command= templated_command,
        )
