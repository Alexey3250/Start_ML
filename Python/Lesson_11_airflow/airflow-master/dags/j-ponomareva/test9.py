from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime


def testing_jp_push(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value=xcom test
    )

def testing_jp_pull(ti)
    ti.xcom_pull(
        key="sample_xcom_key",
        task_ids = 'jp_push01'
    )
    print(xcom test)


with DAG(
    'hw_9_j_ponomareva_01',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG_jp9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['j_pon_09'],
) as dag:
    t1 = PythonOperator(
        task_id = 'jp_push01'.,
        python_callable=testing_jp_push
    )
    t2 = PythonOperator(
        task_id = 'jp_pull01',
        python_callable=testing_jp_pull
    )
    t1 >> t2
