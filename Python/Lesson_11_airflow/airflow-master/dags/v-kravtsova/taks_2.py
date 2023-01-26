from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def ds_input(ds, **kwargs):
    print(ds)
    return ds


with DAG(
    'hw_2_v-kravtsova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    description = 'hw_2',
    start_date = datetime(2022, 12, 23),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False

) as dag:
    t1 = BashOperator(
        task_id='pwd_printing',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='ds_printing',
        python_callable=ds_input
    )

t1>>t2