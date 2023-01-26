from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'chemelson_hw5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='chemelson_hw5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['chemelson_hw5']
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """
    )

    t1 = BashOperator(
        task_id='templated_bash_command',
        bash_command=templated_command
    )
