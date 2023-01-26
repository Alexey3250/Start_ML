from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


with DAG(
        'pobol_first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date = datetime(2022, 12, 24),
        schedule_interval = timedelta(days=1)
        ) as dag:
    

    def push_xcom(ti):

        return "Airflow tracks everything"

    def print_xcom(ti):

        result = ti.xcom_pull(
                key='return_value',
                task_ids='python_print'
                )

        print(result)

    
    python_push = PythonOperator(
            task_id = 'python_print',
            python_callable = push_xcom
            )

    python_print = PythonOperator(
            task_id = 'python_pull',
            python_callable = print_xcom 
            )

    python_push >> python_print
