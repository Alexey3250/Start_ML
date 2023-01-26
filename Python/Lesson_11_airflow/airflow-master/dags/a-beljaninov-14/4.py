from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def print_context(ds, **kwargs):
    task_number = kwargs['task_number']
    print(f"task number is: {task_number}")
    return 'Whatever you return gets printed in the logs'


with DAG(
        '2_a-beljaninov-14',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=5),
        default_args=default_args,
        catchup=False
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
        task_id='print_info',
        bash_command=templated_command,
    )