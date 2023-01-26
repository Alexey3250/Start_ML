from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
with DAG(
    # Название
    'i_Nikolaev_task_13',
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
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
        def decide_which_path():
                if Variable.get("is_startml") == 'True':
                        return "startml_desc"
                else:
                        return "not_startml_desc"
        def pr1():
                print("StartML is a starter course for ambitious people")
        def pr2():
                print('Not a startML course, sorry')
        task1 = DummyOperator(
                task_id = 'before_branching'
        )
        task2 = BranchPythonOperator(
                task_id='determine_course',
                python_callable=decide_which_path
        )
        task3 = PythonOperator(
                task_id='startml_desc',  # в id можно делать все, что разрешают строки в python
                python_callable=pr1
        )
        task4 = PythonOperator(
                task_id='not_startml_desc',  # в id можно делать все, что разрешают строки в python
                python_callable=pr2
        )
        task5 = DummyOperator(
                task_id='after_branching',  # в id можно делать все, что разрешают строки в python
        )
        task1 >> task2 >> [task3, task4] >> task5