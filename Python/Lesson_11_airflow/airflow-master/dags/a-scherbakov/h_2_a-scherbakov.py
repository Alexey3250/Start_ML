from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'lesson2',
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
    description='Lesson 2',
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['les2'],
) as dag:
    t1 = BashOperator(
    task_id='print_date',  # id, будет отображаться в интерфейсе
    bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    def print_ds(ds):
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    	print(ds)
    	return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
    	task_id='print_ds',  # нужен task_id, как и всем операторам
    	python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2
