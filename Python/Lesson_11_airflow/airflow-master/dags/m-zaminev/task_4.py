from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'm-zaminev_task_4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="m-zaminev_task_4",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['m-zaminev_task_4'],
) as dag:
    for i in range(10):
        task1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f"echo {i}"
        )

        task1.doc_md = dedent(
            """
            ## Документация
            Первые **10 задач** типа *BashOperator*.  
            Выполняется команда, использующая переменную цикла `"f"echo {i}"`   
            """
        )


    def print_task_number(task_number):
        print(f'task number is -  {task_number}')


    for i in range(20):
        task2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        task2.doc_md = dedent(
            """
            ## Документация
            Следующие **20 задач** типа *PythonOperator*,  
            Выполняется команда, использующая переменную цикла.  
            Переменная передается через `op_kwargs` и принимается на стороне функции.   
            Печатается `"task number is: {task_number}"`,
            где task_number - номер задания из цикла.
            """
        )

task1 >> task2
