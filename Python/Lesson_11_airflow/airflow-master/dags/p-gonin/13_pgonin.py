from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

with DAG(
    'hw13_pgonin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    ,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['pgonin']
) as dag:


    def choose():       
        if Variable.get("is_startml") == "True":
            return "startml_desc"
        return "not_startml_desc"

    def start_ml():       
        print('StartML is a starter course for ambitious people')

    def not_start_ml():       
        print('Not a startML course, sorry')

    task_choose = BranchPythonOperator(
        task_id = 'task_choose',
        python_callable=choose
    )

    start_ml_task = PythonOperator(
        task_id='startml_desc',  
        python_callable=start_ml
    )

    not_start_ml_task = PythonOperator(
        task_id='not_startml_desc',  
        python_callable=not_start_ml
    )

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    start >> task_choose >> end 
