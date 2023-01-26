from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def phrase():
    return('Airflow tracks everything')

def get_phrase(ti):
    ti.xcom_pull(
        key='return_value',
        task_ids='print_phrase'
    )

with DAG(
        'NG_eigth',
        default_args={
            'depends_on_past': False,
            'email': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='DAG for Task 08',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 23),
) as dag:
    
    t1 = PythonOperator(
        task_id = 'print_phrase',
        python_callable=phrase,
        )

    t2 = PythonOperator(
        task_id = 'print',
        python_callable= get_phrase,
        )

    t1 >> t2
