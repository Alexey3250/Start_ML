from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(

    'HW_12_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_12_e-dracheva'],
    ) as dag:
    
    def var():
        is_startml = Variable.get("is_startml")
        print(is_startml)
    
    t1 = PythonOperator(
        task_id="variable",
        python_callable=var
        )
    t1