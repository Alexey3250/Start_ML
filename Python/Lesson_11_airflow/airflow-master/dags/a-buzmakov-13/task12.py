from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_var():
    from airflow.models import Variable
    result = Variable.get("is_startml")
    print(result)

with DAG(
    'a-buzmakov-13_task_12',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_12'],
) as dag:               
    a = PythonOperator(
        task_id="Print_get",
        python_callable=get_var
    )
