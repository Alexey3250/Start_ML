from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'number_7_1',
    default_args={
        'depends_on_past': False,
        'email':['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Bash + Python operators',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:



    for i in range(10):
        t1 = BashOperator(
            task_id='echo'+str(i),
            bash_command=f'echo {i}'
    )


    def print_func(task_number, ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        return f"task number is: {task_number}"


    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print'+str(i),
            python_callable=print_func,
            op_kwargs={'task_number': i}
    )

    t2.doc_md = dedent(
        """
            #Функция для использования в `PythonOperator`
            ##*Печатает `номер` задания*
            ###Например, **task1**
        """
    )

    t1 >> t2
