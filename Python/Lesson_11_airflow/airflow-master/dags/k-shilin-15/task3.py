'''
Создайте новый DAG и объявите в нем 30 задач. Первые 10 задач сделайте типа BashOperator
и выполните в них произвольную команду, так или иначе использующую переменную цикла (например, можете указать f"echo {i}").

Оставшиеся 20 задач должны быть PythonOperator, при этом функция должна задействовать переменную из цикла.
Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции.
Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла.
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'k-shilin-15_task3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 26),
    catchup=False
) as dag:
    for i in range(10):
        task = BashOperator(
            task_id = 'task_'+str(i),
            bash_command = f"echo {i}"
        )

    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(10, 30):
        task = PythonOperator(
            task_id = 'task_number_'+str(i),
            python_callable = print_task_number,
            op_kwargs = {'task_number': i}
        )
