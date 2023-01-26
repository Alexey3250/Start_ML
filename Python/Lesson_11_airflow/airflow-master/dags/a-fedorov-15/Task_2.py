
from datetime import datetime, timedelta

from textwrap import dedent


from airflow import DAG

from airflow import PythonOperator


from airflow.operators.bash import BashOperator

with DAG(

    'tutorial',

    default_args={
        'depends_on_past': False,

        'email': ['airflow@example.com'],

        'email_on_failure': False,
        'email_on_retry': False,

        'retries': 1,

        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='A simple tutorial DAG',

    schedule_interval=timedelta(days=1),

    start_date=datetime(2022, 1, 1),

    catchup=False,

    tags=['example'],

) as dag:

    t1 = BashOperator(

        task_id='print_date',  
        
        bash_command='pwd',  
    )


    def print_context(ds, **kwargs):
    
        print(ds)


    run_this = PythonOperator(

    task_id='print_the_context',  # нужен task_id, как и всем операторам

    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию

    )
 