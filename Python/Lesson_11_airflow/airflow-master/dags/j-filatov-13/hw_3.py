from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_j-filatov-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='This is a second excercise.BO and PO in cycle.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup=False,
    tags=['hw_3_j-filatov-13'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='bo_print_var' + str(i),
            bash_command=f"echo {i}",
        )

        t1.doc_md = dedent(
            """\
            #### Task 1 Documentation
            This **task** named t1 doing *really* important thing.\
            # It prints number almost 10 *times*!!!

            Here is the code `bash_command=f"echo {i}"`
            # It's **amazing**! Isn't it?
            """
        )

    def print_numbers(task_number):
        print(f"task number is: {task_number}")
        return f"I printed {number} successfully!"

    for i in range(20):
        t2 = PythonOperator(
            task_id='po_print_task_number' + str(i),
            python_callable=print_numbers,
            op_kwargs={'task_number': i},
        )

    t1 >> t2
