from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def set_return_val():
    return "Airflow tracks everything"


def get_return_val(ti):
    return_val = ti.xcom_pull(
        key='return_value',
        task_ids='set_return_val'
    )
    print(f"return val: {return_val}")


with DAG(
        's_pletnev_task_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_10_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 21),
        catchup=False,
        tags=['task_10'],
) as dag:
    set_task = PythonOperator(
        task_id='set_return_val',
        python_callable=set_return_val,
    )
    get_task = PythonOperator(
        task_id='get_return_val',
        python_callable=get_return_val,
    )

    set_task >> get_task
