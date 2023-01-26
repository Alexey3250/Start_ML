from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def return_str(ti):
    return 'Airflow tracks everything'


def print_return_str(ti):
    print(ti.xcom_pull(key='return_value', task_ids='t_return'))


with DAG(
    'hw_9_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
) as dag:
    t1 = PythonOperator(
        task_id='t_return',
        python_callable=return_str
    )

    t2 = PythonOperator(
        task_id='t_print',
        python_callable=print_return_str
    )

    t1 >> t2
