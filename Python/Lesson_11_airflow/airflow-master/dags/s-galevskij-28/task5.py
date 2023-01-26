from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'number_5',
    default_args={
        'depends_on_past': False,
        'email':['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Bash + Python operators',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:

    templated_command=dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}" 
    {% endfor %} 
    """
    )

    t1 = BashOperator(
        task_id='templated',
        bash_command=templated_command
    )

    t1