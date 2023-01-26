from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_12_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['i-djatlov']
) as dag:
    def get_var():
        from airflow.models import Variable
        var = Variable.get("is_startml")
        print(var)
    t1 = PythonOperator(
        task_id='print_var',
        python_callable=get_var,
    )