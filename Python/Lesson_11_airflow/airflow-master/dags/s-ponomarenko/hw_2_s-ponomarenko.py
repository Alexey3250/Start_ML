# s-ponomarenko мой логин

#Первый DAG
# Напишите DAG, который будет содержать BashOperator и PythonOperator.
# В функции PythonOperator примите аргумент ds и распечатайте его.
# Можете распечатать дополнительно любое другое сообщение.
#
# В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow.
# Результат может оказаться неожиданным, не пугайтесь -
# Airflow может запускать ваши задачи на разных машинах или контейнерах
# с разными настройками и путями по умолчанию.
#
# Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.

from datetime import datetime, timedelta
from textwrap import dedent

#Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

#Операторы - это кирпичики DAG, оня являются звеньями в графе.
#Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2_s-ponomarenko',
    #Параметры по умолчанию для тасков
    default_args={
        # если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале
        'email_on_failure': False,
        # писать ли при автоматическом перезапуске при провале
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасков, а самого DAG)
    description = 'DAG for home work - step 2',
    # Как часто запускать DAG (days=1 - каждый день)
    schedule_interval = timedelta(days=1),
    # С какой даты начать запускать DAG.
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно был запуститься.
    # Не всегда совпадает с датой на вашем компьютере.
    start_date = datetime(2022,11,15),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # Теги, способы помечать DAG
    tags = ['homework_s-ponomarenko'],
) as dag:

    # t1, t2, t3 - это операторы, они формируют таски. Таски формируют даг.
    t1 = BashOperator(
        task_id = 'bash_operator_hw_2_s-ponomarenko', # id - будет отображаться в интерфейсе
        bash_command = 'pwd', # выведет директорию, где выполняется ваш код Airflow
    )

    def print_context(ds):
        """ Пример PythonOperator"""
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'let it print in the log'

    run_this = PythonOperator(
        task_id='python_operator_hw_2_s-ponomarenko', # Также указываем task id
        # свойственнен только для PythonOperator - передаем саму функцию
        python_callable=print_context,
    )

    # А вот так в DAG указывается последовательность задач
    t1 >> run_this