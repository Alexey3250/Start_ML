from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_2_a-sharova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 8, 21),
    catchup=False,
    tags=['examples'],
) as dag:

    templ_command = dedent(
        """ 
        {% for i in range(5) %}
            echo "{{ ts }}" 
        {% endfor %} 
        echo "{{ run_id }}" 
        """
    )

    t1 = BashOperator(
        task_id='templ_command',
        bash_command=templ_command
    )
