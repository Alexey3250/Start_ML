"""

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'my_dag_task_3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo_for_' + str(i),
            #bash_command=f"echo {i}" # для задания 2
            env={'NUMBER' : str(i)},
            bash_command="echo $NUMBER"
        )

    def print_task_number(ts, run_id, **kwargs):

        print(f"task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number' : i}
        )

    t1.doc_md = dedent(
    """\
    # Task Documentation
    ###`This is code`
    ###*This is italic*
    ###**This is bold**
    """
    ) 
    

    t1 >> t2
