from datetime import datetime, timedelta #11
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    'hw_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 8, 17),
    catchup=False,
    tags=['no_tag'],
) as dag:
    
    t1 = PythonOperator(task_id='PythonOperator', python_callable=lambda: print(Variable.get("is_startml")))
    
    t1