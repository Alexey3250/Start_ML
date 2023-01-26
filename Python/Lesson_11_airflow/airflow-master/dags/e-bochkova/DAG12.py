from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


def get_variable():
    from airflow.models import Variable
    print(Variable.get("is_startml"))


with DAG(
    'hw_12_e-bochkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description = 'A simple homework DAG',
    # Как часто запускать DAG
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup = False,
    # теги, способ помечать даги
    tags = ['homework12'],
) as dag:

    t1 = PythonOperator(
        task_id = 'get_variable',
        python_callable = get_variable
    )