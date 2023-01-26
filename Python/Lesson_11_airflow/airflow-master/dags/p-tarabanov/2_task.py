from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds, **kwargs):
    """Print ds"""
    print(f'kwargs {kwargs}')
    print(f'ds {ds}')
    return 'Print kwargs and ds'

with DAG(
    'hw_2_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='2 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['BashOperator and PythonOperator'],
) as dag:

    t1 = BashOperator(
        task_id='cur_dir',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
        task_id='print_ds',  # нужен task_id, как и всем операторам
        python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
    )
    
    dag.doc_md = """ Docs of
    BashOperator and PythonOperator
    """
    
    t1 >> t2
    