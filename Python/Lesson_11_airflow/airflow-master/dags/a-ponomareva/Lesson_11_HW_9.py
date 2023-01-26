"""
Сделайте новый DAG, содержащий два Python оператора.
Первый PythonOperator должен класть в XCom значение "xcom test" по ключу "sample_xcom_key".
Второй PythonOperator должен доставать это значение и печатать его. Настройте правильно последовательность операторов.
Посмотрите внимательно, какие аргументы мы принимали в функции, когда работали с XCom.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta




def push_data(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test')


def pull_data(ti):
    data = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_one_example')
    print(data)


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_9_ponomareva',
    default_args=default_args,
    description='DAG for HW_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

    push_data_task = PythonOperator(
        task_id='push_one_example',
        python_callable=push_data,
    )
    pull_data_task = PythonOperator(
        task_id='pull_one_example',
        python_callable=pull_data,
    )

    push_data_task >> pull_data_task