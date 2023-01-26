from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

is_startml = Variable.get("is_startml")

def get_value():
    print(is_startml)

with DAG(
    'n-ubozhenko-10-lesson-11-step-9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 17),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='get_the_value',
        python_callable=get_value
    )

t1




