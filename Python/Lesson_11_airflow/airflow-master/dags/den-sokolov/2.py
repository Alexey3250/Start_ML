from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'den_sokolov_step_2',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 7, 18), 
    catchup=False,
    tags=['den-sokolov'],
) as dag:
    
    def print_smthg(task_number):
        print(f"task number is: {task_number}")

    
    for i in range(10):
        t1 = BashOperator(
            task_id='task_bash_' + str(i),
            bash_command=f"echo {i}",
        )


    for i in range(20):
        t2 = PythonOperator(
            task_id='task_python_' + str(i+10), 
            op_kwargs={'task_number': i+10},
            python_callable=print_smthg
        )


    t1 >> t2