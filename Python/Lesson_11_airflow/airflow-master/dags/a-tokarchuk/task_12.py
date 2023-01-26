
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable



with DAG(
    'my_dag_task_3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    def print_var():
        
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
        task_id = 'print_var',
        python_callable=print_var
    )

    t1







