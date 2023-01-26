from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def x_com_push(ti):
    return "Airflow tracks everything"

def x_com_pull(ti):

    v = ti.xcom_pull(
        key='return_value',
        task_ids='x_com_push'
    )
    print(v)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_9_o_bulaeva_10',
    start_date=datetime(2022, 7, 24),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'x_com_push',
        python_callable=x_com_push
    )
    t2 = PythonOperator(
        task_id = 'x_com_pull',
        python_callable=x_com_pull
    )

    t1 >> t2