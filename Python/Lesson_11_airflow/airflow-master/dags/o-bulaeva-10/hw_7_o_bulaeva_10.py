from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def task_number_printer(task_number, ts, run_id):
    print(f"task number is: {task_number}")
    print(ts)
    print(run_id) 

with DAG('hw_7_o_bulaeva_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries':1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'Second DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 7, 24),
        catchup = False) as dag:

    for i in range(20):
        python_task = PythonOperator(
        task_id='python_task_number_' + str(i),  
        python_callable=task_number_printer,
        op_kwargs={'task_number': i}
        )
