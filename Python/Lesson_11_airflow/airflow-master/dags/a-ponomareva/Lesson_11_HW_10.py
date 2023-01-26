'''
Создайте новый DAG, содержащий два PythonOperator. Первый оператор должен вызвать функцию,
возвращающую строку "Airflow tracks everything" (без примерения XCom).
Второй оператор должен получить эту строку через XCom.
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



def push_data():
    return "Airflow tracks everything"


def pull_data(ti):
    res = ti.xcom_pull(
        key='return_value',
        task_ids='task_push_data')
    print(res)


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_10_ponomareva',
    default_args=default_args,
    description='DAG for HW_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

     task_push = PythonOperator(
         task_id='task_push_data',
         python_callable=push_data,
     )

     task_pull = PythonOperator(
         task_id='task_pull_data',
         python_callable=pull_data,
     )

     task_push >> task_pull


