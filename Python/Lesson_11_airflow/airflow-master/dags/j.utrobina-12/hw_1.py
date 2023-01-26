"""
Test documentation
"""
from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_context(ds):
    
    print(ds)
    return 'Whatever you return gets printed in the logs'


with DAG(
    'hw_1_example',
    # Параметры по умолчанию для тасок
    
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    
    
    description='A simple hw_1 DAG',
   
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 21),
    
    catchup=False
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    ),
    t2 = PythonOperator(
        task_id='print_dt',
        python_callable=print_context
    )

    
t1 >> t2

    
   