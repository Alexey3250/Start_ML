"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'logofios_task_3',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,  # Если прошлые запуски упали, надо ли ждать их успеха
        'email': ['logofios@gmail.com'],  # Кому писать при провале
        'email_on_failure': False,  # А писать ли вообще при провале?
        'email_on_retry': False,  # Писать ли при автоматическом перезапуске по провалу
        'retries': 1,  # Сколько раз пытаться запустить, далее помечать как failed
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),
    },
    description='Task 3',  # Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 20),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['logofios_task_3'],
) as dag:

    # Генерируем таски в цикле - так тоже можно
    for i in range(10):
        # Каждый таск будет спать некое количество секунд
        task_bash = BashOperator(
            # в id можно делать все, что разрешают строки в python
            task_id='task_bash_' + str(i),
            bash_command = f"echo {i}"
        )

    def print_task_id(task_number):
        return print(f'task number is: {task_number}')

    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        task_python = PythonOperator(
            # в id можно делать все, что разрешают строки в python
            task_id='task_python_' + str(i),
            python_callable=print_task_id,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )

    task_bash >> task_python
