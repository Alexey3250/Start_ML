from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'LeoAzm_3',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['lv.azmanov@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),

    },
    description="task2",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 21),
    catchup=False,
    tags=['task3'],
) as dag:


        def print_context(task_number:int ):
                print(f"task number is: {task_number}")
                return f"task number is: {task_number}"
        for i in range(20):
                t1 = PythonOperator(
                        task_id='print_i'+str(i),  # ????? task_id, ??? ? ???? ??????????
                        python_callable=print_context,
                        op_kwargs={"task":i},)
        for j in range(10):
                t2 = BashOperator(
                        task_id="echo_command"+str(j),
                        bash_command= f"echo {j}",)
        t2>>t1