from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    'hw_5_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 17),
    catchup=False,
    tags=['example']
) as dag:
    func = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )
    t1 = BashOperator(
        task_id='temlated',
        bash_command=func,
    )