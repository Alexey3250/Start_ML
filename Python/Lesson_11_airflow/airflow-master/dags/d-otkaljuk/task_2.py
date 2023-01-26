from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
# Создаем DAG. DAG - это инструкция, как выполнять процесс обработки оператора (таска)
with DAG(
    # название
'hw_2_d-otkaljuk',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='Py and Bush operation',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 10, 29),
catchup=False,
tags=['hw_2_d_otkaljuk'],
) as dag:   # Операторы - это кирпичики DAG, они являются звеньями в графе. В них прописывается команды на исполнение
    t1 = BashOperator(
        task_id="t1_pwd",
        bash_command='pwd',
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
        task_id='t2_ds',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2