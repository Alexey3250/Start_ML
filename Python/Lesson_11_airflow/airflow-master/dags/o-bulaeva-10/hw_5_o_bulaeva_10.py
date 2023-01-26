from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG('hw_5_o_bulaeva_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries':1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'Second DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 7, 24),
        catchup = False) as dag:

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id}}"
    {% endfor %}
    """)

    bash_task = BashOperator(
    	task_id = "templated_task",
    	bash_command = templated_command
    	)
