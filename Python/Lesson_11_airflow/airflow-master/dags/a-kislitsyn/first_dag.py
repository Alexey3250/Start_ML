from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'first_dag_kislitsyn',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'DAG, который будет содержать BashOperator и PythonOperator. В функции PythonOperator примите аргумент ds и распечатайте его. '
                  'Можете распечатать дополнительно любое другое сообщение. В BashOperator выполните команду pwd',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 11),
    catchup=False,
    tags=['kislitsyn-1'],
) as dag:
    def print_arg_ds(ds, **kwargs):
        print(ds)

    task1 = BashOperator(
        task_id = 'current_directory',
        bash_command = 'pwd',
    )
    task2 = PythonOperator(
        task_id = 'print_arg_ds',
        python_callable = print_arg_ds,
    )

    task1 >> task2
