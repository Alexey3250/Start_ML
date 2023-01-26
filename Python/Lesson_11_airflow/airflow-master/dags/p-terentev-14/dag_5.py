from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
        'hw5_p-terentev-14',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        ,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['dag_5']
) as dag:
    templated_command = dedent(
        """
         {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
         {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='print_ts',
        bash_command=templated_command,
        dag=dag,
    )