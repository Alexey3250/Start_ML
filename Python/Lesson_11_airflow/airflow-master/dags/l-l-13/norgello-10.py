from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def printi():
    return "Airflow tracks everything"

def load(ti):
    return print(ti.xcom_pull(task_ids='comm_pwd'))

with DAG(
    'norgello-10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='first task in lesson №11',
    schedule_interval=timedelta(days=3650),
    start_date=datetime(2022, 10, 20),
    catchup=False
) as dag:
    m1=PythonOperator(
        task_id='comm_pwd',
        python_callable=printi)

    m2=PythonOperator(
        task_id='logical_date',
        python_callable=load)
m1>>m2