from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'step_2_n_gusarova',
    
    default_args={
        'depends_on_past': False,        
        'email': ['airflow@example.com'],        
        'email_on_failure': False,        
        'email_on_retry': False,        
        'retries': 1,        
        'retry_delay': timedelta(minutes=5),  
    },
    
    
    description='Step 2',
    schedule_interval=timedelta(days=1),
    
    start_date=datetime(2022, 1, 1),
    
    catchup=False,
    
    tags=['step2'],
) as dag:
    t1 = BashOperator(
    task_id='print_date',  
    bash_command='pwd',  
    )

    def print_ds(ds):
    
    	print(ds)
    	return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
    	task_id='print_ds',  
    	python_callable=print_ds,  
    )

    t1 >> t2
