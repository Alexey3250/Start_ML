from textwrap import dedent
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow import DAG


with DAG(
        'hw_5_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 30),
        catchup=False,
        tags=['hw_5_n-anufriev']
) as dag:
    bash_command_echo = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
    """)

    t1 = BashOperator(
        task_id='hw_5_n_besedin_14',
        depends_on_past=False,
        bash_command=bash_command_echo
    )
