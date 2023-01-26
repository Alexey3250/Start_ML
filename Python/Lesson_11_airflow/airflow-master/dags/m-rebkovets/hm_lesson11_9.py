from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator


def push(ti):
    ti.xcom_push(key='sample_xcom_key', value='xcom test')


def puller(ti):
    pulled_value = ti.xcom_pull(key='sample_xcom_key', task_ids='push')
    print(pulled_value)


with DAG(
    '11_9_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],

) as dag:
    t1 = PythonOperator(
        task_id="push",
        python_callable=push,
        )
    t2 = PythonOperator(
        task_id="puller",
        python_callable=puller,
    )

    t1 >> t2
