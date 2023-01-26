from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_var():
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        's_pletnev_task_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_12_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 21),
        catchup=False,
        tags=['task_12'],
) as dag:
    set_xcom_test_task = PythonOperator(
        task_id='print_var_task',
        python_callable=print_var,
    )
