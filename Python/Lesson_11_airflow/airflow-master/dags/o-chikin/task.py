from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def get_info(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom tes",
    )

def print_info(ti):
    result = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="get_info",
    )
    print(result)

with DAG(
    'o-chikin_task9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='task9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['Oleg_Chikin_DAG']
) as dag:
    t1 = PythonOperator(
        task_id = 'get_info',
        python_callable=get_info
    )
    t2 = PythonOperator(
        task_id = 'print_info',
        python_callable=print_info
    )

    t1 >> t2
