from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'den_sokolov_step_8',
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
    start_date=datetime(2022, 7, 18), 
    catchup=False,
    tags=['den-sokolov'],
) as dag:
    

    def push_value(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test')
        
    def pull_value(ti):
        xcom_test = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='task_xcom_push')
        print(f'This is our xcom test value - {xcom_test}')
    
    t1 = PythonOperator(
        task_id='task_xcom_push', 
        python_callable=push_value
    )
    
    t2 = PythonOperator(
        task_id='task_xcom_pull', 
        python_callable=pull_value
    )    


    t1 >> t2