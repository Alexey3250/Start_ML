from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
        'kolomiets_11_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description="Lerning DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 21),
        catchup=False,
) as dag:
    templated_command = dedent(
        '''
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{run_id }}
        {% endfor %}
        '''
    )

    t1 = BashOperator(
        task_id='loop_bash',
        bash_command=templated_command
    )

    t1
