from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_4',
    default_args={
        'depends_on_past': False,
        'email': ['study.all.c@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='exersise 4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 18),
    catchup=False,
    tags=['templates'],
) as dag:
    template_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id}}"
        {% endfor %}
        """
    )

        t1 = BashOperator(
            task_id='print_ts_task_id',
            bash_command=template_command,
        )

