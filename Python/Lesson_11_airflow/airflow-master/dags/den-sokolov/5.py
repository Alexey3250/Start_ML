from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    'den_sokolov_step_5',
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
    
   
    for i in range(10):
        NUMBER = i
        t1 = BashOperator(
            task_id='task_bash_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
        )


    t1