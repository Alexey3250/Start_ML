from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_2_e-bochkova',
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
    tags = ['homework2'],
) as dag:
    t1 = BashOperator(
        task_id="print_pwd",
        bash_command="pwd",
    )

    def print_context(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id="print_ds",
        python_callable=print_context,
    )

    t1 >> t2
