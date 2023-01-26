from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_xcom(ti):
    ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='simple_xcom_put'
    )


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'e-kolomiets_xcom',
        default_args=default_args,
        start_date=datetime(2022, 12, 22),
        schedule_interval=timedelta(days=1)
) as dag:
    put_xcom = PythonOperator(
        task_id='simple_xcom_put',
        python_callable=push_xcom,
    )
    get_xcom = PythonOperator(
        task_id='simple_xcom_get',
        python_callable=pull_xcom,
    )

    put_xcom >> get_xcom
