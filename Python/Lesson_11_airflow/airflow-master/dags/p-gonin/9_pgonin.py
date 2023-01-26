from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw9_pgonin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    ,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['pgonin']
) as dag:


    def xcom_push(ti):
        ti.xcom_push(
                key='sample_xcom_key',
                value='xcom test'
            )

    def xcom_pull(ti):
        res = ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='push'
            )
        print(res)

    t1 = PythonOperator(
        task_id='push',  
        python_callable=xcom_push
    )
    
    t2 = PythonOperator(
        task_id='pull',  
        python_callable=xcom_pull
    )

    t1 >> t2
