from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_2',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Lesson 11, homework 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['i_boztayev']

) as dag:

    # t1, t2 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='path_print',  # id, будет отображаться в интерфейсе
        bash_command='pwd' # какую bash команду выполнить в этом таске
    )

    def print_context(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )


    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
