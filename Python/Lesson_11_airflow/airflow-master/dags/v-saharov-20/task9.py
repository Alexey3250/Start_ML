from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def x_push(sample_xcom_key, ti):

    ti.xcom_push(
        key="sample_xcom_key",
        value=sample_xcom_key
    )

def x_pull(ti):
    needed_value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="xcom_push"
    )
    print(needed_value)

with DAG(
        dag_id="task9_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    x_push = PythonOperator(
        task_id="xcom_push",
        python_callable=x_push,
        op_kwargs={"sample_xcom_key": "xcom test"}
    )
    x_pull = PythonOperator(
        task_id="xcom_pull",
        python_callable=x_pull
    )
    x_push >> x_pull