from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        dag_id="task5_final_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    sample_task = dedent("""
    {% for i in range(5) %}
    echo Для каждого "{{ i }}" в диапазоне от 0 до 5 не включительно распечатать значение "{{ ts }}" и затем распечатать значение "{{ run_id }}"

    {% endfor %}
    """)

    bash_tasks = BashOperator(
        task_id=f"print_5_values",
        bash_command=sample_task
    )

