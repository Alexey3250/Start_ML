from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_6_a-shapovalov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Excercise 6 a-shapovalov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_6_a-shapovalov']
) as dag:
    for i in range(1, 11):
        bash = BashOperator(
            task_id=f'bash_command_{i}',
            env={'NUMBER': i},
            bash_command='echo $NUMBER')

    bash.doc_md = dedent(
        """
        Documentation
        # Heading 1
        ## Heading 2
        ### Heading 3
        __bold text__
        **also bold text**
        _italic text_
        *also italic text*
        `some random code line`
        """)


    def print_task_number(task_number):
        print(f'task number is: {task_number}')


    for i in range(11, 31):
        python = PythonOperator(
            task_id=f'python_command_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    bash >> python
