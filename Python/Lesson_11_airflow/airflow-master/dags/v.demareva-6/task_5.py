from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
with DAG\
    (
    "task_5_v_demareva",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "DAG for task #5",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 8, 21),
    catchup = False,
    tags = ["task_5"]
    ) as dag:

    templated_code = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"
        """
    )
    task_1 = BashOperator(
        task_id = "templated_print_ts_run_id",
        bash_command = templated_code
    )

