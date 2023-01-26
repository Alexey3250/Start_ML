from datetime import datetime, timedelta
from airflow import DAG


def print_ds(ds):
    print(ds)
    pass


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'print_context_t2',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:

    t1 = BashOperator(
        task_id='print_path',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
    task_id='print_ds',  # нужен task_id, как и всем операторам
    python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
)

    t1 >> t2
