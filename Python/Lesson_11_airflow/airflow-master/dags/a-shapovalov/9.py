from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def ti_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def ti_pull(ti):
    res = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='task1'
    )
    print(res)


with DAG(
        'hw_9_a-shapovalov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Excercise 9 a-shapovalov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_9_a-shapovalov']
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=ti_push
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=ti_pull
    )

    task1 >> task2
