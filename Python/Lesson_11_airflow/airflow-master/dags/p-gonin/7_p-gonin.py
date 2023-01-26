from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw7_pgonin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    ,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['pgonin']
) as dag:

    # Генерируем таски в цикле - так тоже можно
    for i in range(10):
        task = BashOperator(
            task_id='echo_' + str(i),  # в id можно делать все, что разрешают строки в python
            bash_command=f'echo {i}'
        )
        task

    def my_func(ts,run_id,**op_kwargs):
        tn = op_kwargs['task_number']
        print(f'task number is {tn}')
        print(ts)
        print(run_id)

    # Генерируем таски в цикле - так тоже можно
    for i in range(10,30):
        task = PythonOperator(
            task_id='print_' + str(i),  
            python_callable=my_func,
            op_kwargs={'task_number': i},
        )
        task
