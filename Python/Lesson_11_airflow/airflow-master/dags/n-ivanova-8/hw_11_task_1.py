from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

ds_date = "{{ ds }}"


def print_ds(ds):
    print(ds_date)


with DAG(
        'hw_1_DAG_nivanova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 13),
        catchup=False,
        tags=['hw'],
) as dag:
    print_ds = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
        op_kwargs={'ds': ds_date}
    )

    pwd = BashOperator(
        task_id='pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    # порядок выполнения
    pwd >> print_ds

