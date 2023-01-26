from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'NG_tenth',
    default_args={
        'depends_on_past': False,
        'email': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
   
    description='DAG for Task 10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 23),
    ) as dag:

    def variable():
        from airflow.models import Variable
        is_startml = Variable.get("is_startml")
        print(is_startml)

    task = PythonOperator(
        task_id='startml_variable',
        python_callable=variable,
    ) 
    
    task
