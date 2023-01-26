from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'den_sokolov_step_11',
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
    start_date=datetime(2022, 7, 19), 
    catchup=False,
    tags=['den-sokolov'],
) as dag:

        

    def get_variable():

        from airflow.models import Variable
        is_startml = Variable.get("is_startml")  

        print(is_startml)
            
    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable
    )


    t1