from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'step_3_n_gusarova',
    
    default_args={
        'depends_on_past': False,        
        'email': ['airflow@example.com'],        
        'email_on_failure': False,        
        'email_on_retry': False,        
        'retries': 1,        
        'retry_delay': timedelta(minutes=5),  
    },
    
    
    description='step_3',
    schedule_interval=timedelta(days=1),
    
    start_date=datetime(2022, 1, 1),
    
    catchup=False,
    
    tags=['gusarova_3'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='tasks' + str(i),
            bash_command=f"echo {i}"
        )
    def context(task_number):
        return f"task number is: {task_number}"
    for i in range(20):
        t2 = PythonOperator(
            task_id='python' + str(i),
            python_callable=context,
            op_kwargs={'task_number': i}
        )


    t1 >> t2
