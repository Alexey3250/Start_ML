# испортирую нужные библиотеки и методы
rom datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# описываю DAG
with DAG(
    # уникальное название DAG
    'a-feofanova_lesson_11_hw_step_3',
    # аргументы по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    # описание DAG
    description='DAG with 30 tasks made by means of cycle "for"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 14),
    catchup=False,
    tags=['DAG with 30 tasks'],
) as dag:

    # описываю операторы
    # Первые 10 задач типа BashOperator
    for task_number in range(10):
        t1 = BashOperator(
            task_id=f'bash_cycle_number_{task_number}',
            retries=4,
            bash_command=f"echo {task_number}",
            )

    # Далее пишу PythonOperator
    # описываю функцию для PythonOperator
    # функция принимает на вход номер таска
    # и печатает его
    def print_task_number(task_number):
        return f'task number is: {task_number}'

    # теперь 20 задач типа PythonOperator
    # чтобы функция задействовала переменную из цикла,
    # передаю эту переменную через op_kwargs
    # op_kwargs принимает словарь:
    # {'название_переменной_из_функции': 'название_переменной_из_цикла'}
    for task_n in range(20):
        t2 = PythonOperator(
            task_id=f'python_cycle_number_{task_n}',
            python_callable=print_task_number,
            op_kwargs={'task_number': task_n}
        )

    # Описываю последовательность выполнения тасок
    t1 >> t2
