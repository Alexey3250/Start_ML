from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_var(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_var(ti):
    print(ti.xcom_pull(key='sample_xcom_key', task_ids='t_push'))


with DAG(
    'hw_8_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 8',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
) as dag:
    t1 = PythonOperator(
        task_id='t_push',
        python_callable=push_var
    )

    t2 = PythonOperator(
        task_id='t_pull',
        python_callable=pull_var
    )

    t1 >> t2
