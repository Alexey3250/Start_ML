from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
# Создаем DAG. DAG - это инструкция, как выполнять процесс обработки оператора (таска)
with DAG(
'hw_7_v2_e-poljakov-13', # название DAG
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
},  description='hw_3',  # Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),  # Как часто запускать DAG
    start_date=datetime(2022, 1, 1), # С какой даты начать запускать DAG. Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    catchup=False,  # Запустить за старые даты относительно сегодня,
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    tags=['poljakov-13'],  # теги, способ помечать даги
) as dag:   # Операторы - это кирпичики DAG, они являются звеньями в графе. В них прописывается команды на исполнение


    # функция печати
    def print_number_of_task(ts, run_id, **kwargs):
        """Пример PythonOperator"""
        kwargs.get('task_number')  # получаем значение из аргумента op_kwargs, методом get
        print(f'print_ts_{ts}')
        print(f'print_run_id_{run_id}')
        return "Everything is good"
    for i in range(5):
        python_task = PythonOperator(
            task_id=f"print_{i}",
            python_callable=print_number_of_task,
            op_kwargs={"task_number": i}  # task_number - названием переменной def print_number_of_task под
            # видом **kwargs, i - значение предаваемое в функцию
    )
    python_task