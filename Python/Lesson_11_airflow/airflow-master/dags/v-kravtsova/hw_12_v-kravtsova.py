from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def get_variable(is_startml):
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
    'hw_12_v-kravtsova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    description = 'hw_12',
    start_date = datetime(2022, 12, 23),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable
    )


t1