from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'den_sokolov_step_1',
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
    start_date=datetime(2022, 7, 17), 
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='get_current_directory',
        bash_command='pwd',
    )

    def print_date(ds, **kwargs):
        print(ds)
    
    t2 = PythonOperator(
        task_id='print_current_date', 
        python_callable=print_date, 
    )

    t1 >> t2