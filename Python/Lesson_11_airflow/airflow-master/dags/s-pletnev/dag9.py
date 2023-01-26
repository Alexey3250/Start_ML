from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def set_xcom_test(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )


def get_xcom_test(ti):
    xcom_test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='set_xcom_test'
    )
    print(f"xcom_test: {xcom_test}")


with DAG(
        's_pletnev_task_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_9_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 21),
        catchup=False,
        tags=['task_9'],
) as dag:
    set_xcom_test_task = PythonOperator(
        task_id = 'set_xcom_test',
        python_callable=set_xcom_test,
    )
    get_xcom_test_task = PythonOperator(
        task_id = 'get_xcom_test',
        python_callable=get_xcom_test,
    )

    set_xcom_test_task >> get_xcom_test_task
