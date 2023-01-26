from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('polunina_5',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description='A unit 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['A unit 5'],
) as dag:

    templated_command = dedent(
    """
    {%for i in range(5)%}
        echo "{{ ts }}"
    {% endfor %}    
    {%for i in range(5)%}
        echo "{{ run_id }}"
    {% endfor %}  
    """
    )
    t1 = BashOperator(task_id = 'bash_5', bash_command = templated_command)






