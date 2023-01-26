from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'den_sokolov_step_6',
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
    
    def print_smthg(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(f"ts is: {ts}")
        print(f"task run_id is: {run_id}")
   
    for i in range(20):
        t1 = PythonOperator(
            task_id='task_python_' + str(i), 
            op_kwargs={'task_number': i},
            python_callable=print_smthg
        )


    t1