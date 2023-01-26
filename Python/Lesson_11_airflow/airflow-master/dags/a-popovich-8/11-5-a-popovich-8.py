from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw-5-a-popovich-8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw__a-popovich-8'],
) as dag:

    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='wait_for_all_bash_operators')
    t3 = DummyOperator(task_id="finish_dag")

    for i in range(10):
        task = BashOperator(
            task_id=f'print_date_{i}',
            bash_command='echo "doing magic $NUMBER"',
            env={"NUMBER": str(i)}
        )
        task.doc_md = dedent(
            f"""\
            #### Task {i} documentation
            Используя `Python`, можно достичь **больших** высот в _программировании_
            """
        )
        t2 << task << t1

    def print_context(ds, **kwargs):
        print(kwargs)
        print(f"task number is: {kwargs.get('task_number')}")
        print(ds)
        return 'Whatever you return gets printed in the logs'


    for i in range(20):
        python_task = PythonOperator(
            task_id=f'print_the_context_{i}',
            python_callable=print_context,
            op_kwargs={"task_number": i}
        )
        t3 << python_task << t2
