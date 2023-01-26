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
    'AKA-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:
    for i in range(30):
        if i<10:
            t1 = BashOperator(
                task_id='task'+str(i),
                bash_command="echo $NUMBER",
                env={"NUMBER": i}
            )
        else:
            def print_i(task_number, ts, run_id, **kwargs):
                print(task_number, ts, run_id)

                return 'test_return'
            t2 = PythonOperator(
                task_id='task'+str(i),
                python_callable=print_i,
                op_kwargs={'task_number': i}
            )
    t1.doc_md = dedent(
        """\
    #### Bash Documentation
    You can document your task using the attributes `doc_md` (markdown),
    **doc** (plain text), _doc_rst_
    """)
    t2.doc_md = dedent(
        """\
    #### Python Documentation
    You can document your task using the attributes `doc_md` (markdown),
    **doc** (plain text), _doc_rst_
    """)
    t1 >> t2