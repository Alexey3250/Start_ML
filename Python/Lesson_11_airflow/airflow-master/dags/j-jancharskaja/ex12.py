from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    'j-jancharskaja_12',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My tenth DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['tenth']
) as dag:

    def get_var():
        from airflow.models import Variable

        print(Variable.get('is_startml'))
    
    t1 = PythonOperator(
        task_id = 'get_var',
        python_callable = get_var,
        )
