from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(

    'HW_4_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    
    description='HW 4 EDracheva',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_4_e-dracheva'],
    ) as dag:
  
    def OP(task_number):        
        print(f"task number is: {task_number}")
    
    for i in range(10):
        t1 = BashOperator(
            task_id='task_number_' + str(i),  
            bash_command=f"echo {i}",  
        )
    for j in range(10, 30):
        t2 = PythonOperator(
            task_id = 'task_number_'+ str(j),
            python_callable=OP,
            op_kwargs = {'task_number': j}
                )
    t1.doc_md = dedent("""
        
        text
        _text_
        __text__
        `text`
        #text
        **text**
        {{{text}}}
        ''курсив''
        """
        ) 
    t2.doc_md = dedent("""
        
        text
        _text_
        __text__
        `text`
        #text
        **text**
        {{{text}}}
        ''курсив''
        """
        ) 
    t1 >> t2