from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
)

with DAG(
    'hw5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw5 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['BH_hw5'],
) as dag:
    
    
    t1 = BashOperator(
        task_id='template_dag',  
        depends_on_past=False,
        bash_command=templated_command
    )
    
    t1