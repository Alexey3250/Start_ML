from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'hw_5_r-kulaev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_5_r-kulaev',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 27),
        catchup=False,
        tags=['hw_5_r-kulaev']
) as dag:
    tmpl_cmd = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """
    )

    t1 = BashOperator(
        task_id='tmpl_bash_command',
        bash_command=tmpl_cmd
    )
