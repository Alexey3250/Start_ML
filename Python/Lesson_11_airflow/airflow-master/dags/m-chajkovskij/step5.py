from datetime import datetime, timedelta
from textwrap import dedent
# To declare a DAG the DAG class of the airflow must be imported
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "task_11.3",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes =5),
    },
    description='m-chajkovskij_11.5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['homework']
)as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id}}"
    {% endfor %}
    """
    )
    t = BashOperator(task_id='bash_11.5', bash_command=templated_command)