from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    '7_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='7 dag',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 9, 21),
    catchup=False,
    tags=['7 dag']
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id='bashop' + str(i),
            bash_command=f'echo $NUMBER',
            env={'NUMBER': str(i)},
        )

    def python_func(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(20):
        task2=PythonOperator(
            task_id='pythop' + str(i),
            python_callable=python_func,
            op_kwargs={'task_number': i},
        )

    task.doc_md = dedent(
        """\
        # Task documentation
        **This** is *generic* documentation for this `task`     
        """
    )