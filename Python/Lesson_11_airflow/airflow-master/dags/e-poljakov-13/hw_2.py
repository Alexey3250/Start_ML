from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
# Создаем DAG. DAG - это инструкция, как выполнять процесс обработки оператора (таска)
with DAG(
'hw_2_e-poljakov-13', # название DAG
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
},  description='hw_2',  # Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),  # Как часто запускать DAG
    start_date=datetime(2022, 1, 1), # С какой даты начать запускать DAG. Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    catchup=False,  # Запустить за старые даты относительно сегодня,
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    tags=['hw_2_e-poljakov-13'],  # теги, способ помечать даги
) as dag:   # Операторы - это кирпичики DAG, они являются звеньями в графе. В них прописывается команды на исполнение
    t1 = BashOperator(
        task_id="show_pwd",
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    def print_context(ds, **kwargs):
        """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно
        #print(kwargs)
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)

    t2 = PythonOperator(
        task_id='print_ds',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )
    t1 >> t2

