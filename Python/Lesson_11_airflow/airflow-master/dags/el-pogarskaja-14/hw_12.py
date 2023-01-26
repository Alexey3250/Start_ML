from airflow import DAG
from textwrap import dedent
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

with DAG(
        'el-pogarskaja-14_12',
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
        start_date=datetime(2021, 1, 1),
        catchup=False,
        ) as dag:
    
    def get_variable():
        from airflow.models import Variable
        is_startml=Variable.get("is_startml") #gets variable is_startml from Airflow
        print(is_startml)

    t1 = PythonOperator(
            task_id = 'print_is_startml',
            python_callable = get_variable,
            )

