from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

with DAG(
    'den_sokolov_step_12',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 7, 21), 
    catchup=False,
    tags=['den-sokolov'],
) as dag:

        

    def get_variable():

        from airflow.models import Variable
        is_startml = Variable.get("is_startml")  
        if is_startml == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'            
                      
    def variable_is_true():
        
        print("StartML is a starter course for ambitious people")
    
    def variable_is_false():
        
        print("Not a startML course, sorry")

    run_this_first = DummyOperator(
        task_id='run_this_first', 
        dag=dag)
    
    branch_op = BranchPythonOperator(
        task_id='branch_task',
        # provide_context=True,
        python_callable=get_variable,
        dag=dag)
    
    true_op = PythonOperator(
        task_id='startml_desc',
        python_callable=get_variable,
        dag=dag
    )

    false_op = PythonOperator(
        task_id='not_startml_desc',
        python_callable=get_variable,
        dag=dag
    )    
    
    run_this_last = DummyOperator(
        task_id='run_this_last', 
        dag=dag)
        

    run_this_first >> branch_op >> [true_op, false_op] >> run_this_last