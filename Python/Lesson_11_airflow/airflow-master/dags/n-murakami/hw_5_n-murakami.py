from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator



with DAG(
    'hw_5_n-murakami',
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_5_n-murakami'],
) as dag:
    temlated_command=dedent("""
    {% for i in range(5) %}
        echo "{{ts}}"
        echo "{{run_id}}"
    {% endfor %}
    """)
    t=BashOperator(
        task_id='templated_bash',
        bash_command=temlated_command
    )
