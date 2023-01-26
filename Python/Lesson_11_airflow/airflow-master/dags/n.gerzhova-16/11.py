from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'NG_eleventh',
    default_args={
        'depends_on_past': False,
        'email': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
   
    description='DAG for Task 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 23),
    ) as dag:

    def to_task():
        from airflow.models import Variable
        variable = Variable.get("is_startml")
        if variable == 'True':
            return 'startml_desc' 
        else:
            return 'not_startml_desc'
    
    def startml():
        print('StartML is a starter course for ambitious people')

    def not_startml():
        print('Not a startML course, sorry')


    t0 = DummyOperator(
        task_id='before_branching',
        )

    tX = BranchPythonOperator(
        task_id='t1_or_t2',
        python_callable=to_task,
        )     
    
    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml,
        ) 


    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml,
        ) 

    t3 = DummyOperator(
        task_id="after_branching",
        )
    


    t0 >> tX >> [t1, t2] >> t3
