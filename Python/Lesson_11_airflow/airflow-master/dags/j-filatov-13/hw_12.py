from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import RealDictCursor
from airflow.models import Variable

def print_var():
    var = Variable.get("is_startml")
    print (var)

with DAG(
        'hw_12_j-filatov-13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_12_exercise_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 22),
        catchup=False,
        tags=['hw_12_j-filatov-13_tag']
) as dag:

    t1 = PythonOperator(
        task_id='print_val',
        python_callable=print_var,
    )

    t1
