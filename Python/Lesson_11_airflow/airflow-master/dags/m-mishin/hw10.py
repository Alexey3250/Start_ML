from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    '11_10_mishin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw 10',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 9, 21),
    catchup=False,
    tags=['m-mishin']
) as dag:

    def xcom_push():
        return "Airflow tracks everything"

    def xcom_pull(ti):
        text = ti.xcom_pull(
            key='return_value',
            task_ids='xcom_push')

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2
