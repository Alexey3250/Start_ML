"""

"""


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'my_dag_task_2',
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
    def print_context(ds):
        print(ds)


    t1 = BashOperator(
        task_id = 'bash_1',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id = 'python_1',
        python_callable = print_context,
    )

  
    t1 >> t2
