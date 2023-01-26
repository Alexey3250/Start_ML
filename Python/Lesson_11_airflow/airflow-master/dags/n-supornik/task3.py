from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

with DAG('task3', 

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
    description='Solution of task2',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 12, 20),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
    ) as dag:
        def print_context(i, **kwargs):
    
            print('Hello from {kw}'.format(kw=kwargs[i]))
            # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD   
        for i in range(30):
            if i < 10:
                t1 = BashOperator(
                task_id=f'print{i}',
                bash_command=f"echo {i}",
                )
            else:
                           
                t2 = PythonOperator(
                task_id=f'print_the_context{i}',  # нужен task_id, как и всем операторам
                python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
                op_kwargs={'my_keyword': i},
                )

                t1 >> t2

    