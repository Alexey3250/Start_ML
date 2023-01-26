from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    '11_9_mishin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw 9',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 9, 25),
    catchup=False,
    tags=['m-mishin']
) as dag:

    def get_data(ti):
        data = "xcom test"
        ti.xcom_push(
            key="sample_xcom_key",
            value=data,
        )

    def print_data(ti):
        data = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids="get_data",
        )
        print(data)

    get = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
    )

    prin = PythonOperator(
        task_id="print_data",
        python_callable=print_data,
    )

    get >> prin
