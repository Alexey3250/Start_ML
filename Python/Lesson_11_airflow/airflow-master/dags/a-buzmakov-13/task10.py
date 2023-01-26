from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    'a-buzmakov-13_task_10',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_10'],
) as dag:
    def push_xcom_func(ti):
        return "Airflow tracks everything"


    def pull_xcom_func(ti):
        value_read = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(value_read)


    a1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom_func,
    )

    a2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_func,
    )

    a1 >> a2
