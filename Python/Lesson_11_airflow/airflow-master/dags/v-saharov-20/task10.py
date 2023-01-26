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

def push_airflow_value():
    return "Airflow tracks everything"

def pull_airflow_value(ti):
    needed_value = ti.xcom_pull(key="return_value",task_ids="push_airflow")
    print(needed_value)

with DAG(
        dag_id="task10_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    push_airflow = PythonOperator(
        task_id="push_airflow",
        python_callable=push_airflow_value
    )
    take_airflow = PythonOperator(
        task_id="take_airflow",
        python_callable=pull_airflow_value
    )

    push_airflow >> take_airflow