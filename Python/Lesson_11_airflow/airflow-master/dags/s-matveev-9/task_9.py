from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def set_to_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )


def get_from_xcom(ti):
    xcom_value = ti.xcom_pull(
                    task_ids="hw9_msv_task_set_xcom",
                    key='sample_xcom_key'
                 )
    print(xcom_value)


with DAG(
    "hw_9_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    set_task = PythonOperator(
        task_id=f"hw9_msv_task_set_xcom",
        python_callable=set_to_xcom,
    )
    get_task = PythonOperator(
        task_id=f"hw9_msv_task_get_xcom",
        python_callable=get_from_xcom,
    )

    set_task >> get_task
