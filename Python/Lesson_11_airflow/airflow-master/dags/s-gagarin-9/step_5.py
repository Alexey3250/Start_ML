from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent

templated_command = dedent(
    """{% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
       {% endfor %}
    """
)

with DAG(
        'step_5_gagarin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG_for_step_5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 12),
        catchup=False,
        tags=['step_5_gagarin']
) as dag:
    task = BashOperator(
        task_id='print_ts_run_id',
        bash_command=templated_command
    )
    task.doc_md = dedent(
        """Print ts and run_id via templanting
        """
    )
