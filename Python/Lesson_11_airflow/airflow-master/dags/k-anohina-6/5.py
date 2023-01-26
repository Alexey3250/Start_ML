"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'AKA-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
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
        task_id='task_5',
        bash_command=templated_command
    )

    t1.doc_md = dedent(
        """\
    #### Bash Documentation
    You can document your task using the attributes `doc_md` (markdown),
    **doc** (plain text), _doc_rst_
    """)
    t1