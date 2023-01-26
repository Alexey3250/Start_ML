"""
Print iterator `from 0 to 10` using **BashOperator**
Print task numbers `from 10 to 30` using __PythonOperator__

`doc_md` (markdown),`doc` (_plain text_), `doc_rst`, `doc_json`
"""


from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_4_v-didovik',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_4_v-didovik',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_4_v-didovik'],
) as dag:

    dag.doc_md = __doc__

    for i in range(10):
        t1 = BashOperator(
            task_id='bash_operator_' + str(i),
            bash_command=f"echo {i}",
        )

    t1.doc_md = dedent(
        """
    # Function
      Print in console number of iteration
    # Iteration range
      Iteration range is fixed from 0 to 10 (excluded last one). 
    **bold**
    __semi-bold__
    _italic_
    `code`
    """)

    def print_task_number(task_number):
        print("task number is:", task_number)

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_str_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )


    t1 >> t2

