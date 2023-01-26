from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def ds_input(ds, **kwargs):
    print(ds)
    return ds


with DAG(
    'hw_3_v-kravtsova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    description = 'hw_2',
    start_date = datetime(2022, 12, 23),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False

) as dag:

    for i in range(10):
        task1 = BashOperator(
            task_id='task' + str(i),
            bash_command=f"echo {i+1}"
        )

    task1.doc_md = dedent(
        """\
        #### Task Documentation
        Documentation for `task1`.
        It is **bash operator** for echoing _cycle variable_.
        """
    )
    def task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        task2 = PythonOperator(
            task_id='printing_task_number' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )
    task2.doc_md = dedent(
        """\
        #### Task Documentation
        Documentation for `task2`.
        It is **python operator** for printing _task number_.
        """
    )
