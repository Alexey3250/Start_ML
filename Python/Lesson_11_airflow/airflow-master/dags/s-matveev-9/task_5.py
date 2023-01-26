from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


templated_command = dedent(
                    """ 
                        {% for i in range(5) %}
                            echo "{{ ts }}"
                            echo "{{ run_id }}"
                        {% endfor %}
                
                    """)



with DAG(
    "hw_5_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
        bash_task = BashOperator(
                        task_id="hw5_msv_bash_task",
                        bash_command=templated_command,
                    )
