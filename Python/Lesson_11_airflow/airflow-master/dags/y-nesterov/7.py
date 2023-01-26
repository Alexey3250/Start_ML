from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
with DAG(
    'tutorial',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    def print_text(task_number, ts, run_id):
        print(task_number, ts, run_id)


    # Генерируем таски в цикле - так тоже можно
    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        task2 = PythonOperator(
            task_id='print_for_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_text,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i, 'ts':  "{{ ts }}", 'run_id': "{{ run_id }}"}
        )