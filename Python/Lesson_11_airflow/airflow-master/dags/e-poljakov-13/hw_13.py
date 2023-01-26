from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from datetime import timedelta, datetime


# Функция выбора (в ней заложен алгоритм принятия решения на какую ветку идти, если удовлетворяется условие)
def my_operator():
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"

def print_for_t3_1():
    print("Not a startML course, sorry")

def print_for_t3_2():
    print("StartML is a starter course for ambitious people")


# Создаем DAG. DAG - это инструкция, как выполнять процесс обработки оператора (таска)
with DAG(
'hw_13_e-poljakov-13', # название DAG
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
},  description='hw_13',  # Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),  # Как часто запускать DAG
    start_date=datetime(2022, 1, 1), # С какой даты начать запускать DAG. Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    catchup=False,  # Запустить за старые даты относительно сегодня,
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    tags=['poljakov-13'],  # теги, способ помечать даги
) as dag:   # Операторы - это кирпичики DAG, они являются звеньями в графе. В них прописывается команды на исполнение
    # Такса-заглушка
    t1 = DummyOperator(task_id='before_branching')
    # Таска с BranchPythonOperator, которая вызывает функцию my_operator,
    # где лежит алгоритм выбора на какую таску перейти
    t2 = BranchPythonOperator(task_id='determine_course',
                              python_callable=my_operator)
    # Таска, на которую будет или не будет сделан переход, которая в свою очередь вызовет функцию print_for_t3_1, если
    # будет вызвана
    t3_1 = PythonOperator(task_id="not_startml_desc",
                         python_callable=print_for_t3_1)
    # Таска, на которую будет или не будет сделан переход, которая в свою очередь вызовет функцию print_for_t3_2, если
    # будет вызвана
    t3_2 = PythonOperator(task_id="startml_desc",
                         python_callable=print_for_t3_2)
    # Такса-заглушка
    # t4 = DummyOperator(task_id="after_branching")
    # Таск-заглушка с аргументом перехода И ВЫПОЛНЕНИЯ на неё, после выбора из двух тасок перед ней
    t4 = DummyOperator(task_id="after_branching",
                       trigger_rule="none_failed_or_skipped")


    t1 >> t2 >> [t3_1, t3_2] >> t4