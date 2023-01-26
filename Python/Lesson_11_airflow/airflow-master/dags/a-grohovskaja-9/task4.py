from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'a-grohovskaja-9_hw_4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-grohovskaja-9_hw_4'],
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
        task_id='templated_bash',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1

