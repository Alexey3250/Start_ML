from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    '2_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='2 dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 19),
    catchup=False,
    tags=['2_dag']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo'+str(i),
            bash_command=f"echo {i}")
    
    def get_task_number(task_number):
        print(f"task number is: {task_number}")
    
    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number':i})

    t1 >> t2
