from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'i-morkovkin_hw_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Exercise No.7',
    start_date=datetime(2022, 6, 16)
) as dag:

    def print_task_number(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'bash_task_{i}',
                bash_command="echo $NUMBER",
                env={"NUMBER": i}
            )
        else:
            t2 = PythonOperator(
                task_id=f'python_task_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )

    t1.doc_md = dedent(
        """
        #documentation
        `code = 1`
        *cursive*
        **thicc**
        _курсив_
        __полужирный__
        """
    )

    t1 >> t2

