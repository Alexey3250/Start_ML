from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

# импортирую bash и python операторы
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'lesson_11_hw_step_2',    # название DAG
    # ниже идут параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # описание DAG
    description='DAG for the hw on the 2nd step of the 11th lesson',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 13),
    catchup=False,
    tags=['just practicing making tags']
) as dag:

    # Пишу Bash оператор
    t1 = BashOperator(
        task_id='show_directory',
        depends_on_past=False,
        bash_command='pwd',    # выведет директорию, где выполняется код AirFlow
        retries=2,
    )

    # Ниже документация к оператору t1
    # Функция dedent решает все проблемы с экранированием, отступами
    t1.doc_md = dedent(
    """\
    #### Task Documentation
    Task t1 utilizes **BashOperator** and is intended for
    showing the current directory where the AirFlow code is running
    """
    )

    # Пишу функцию, по которой будет выполняться python оператор
    # ds - строка даты логического выполнения в виде YYYY-MM-DD
    def print_logical_date(ds):
        print(ds)
        print('This message has to be printed right after the "ds"')
        return 'This is not just a print. It is executed with the help of "return" command'

    # Пишу Python оператор
    t2 = PythonOperator(
        task_id='print_logical_date',
        python_callable=print_logical_date, # функция, которую AirFlow будет выполнять
    )

    # Указываю последовательность выполнения операторов
    # Сперва t1, затем t2
    t1 >> t2
