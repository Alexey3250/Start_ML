from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'homework_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2022, 1, 1),
        tags=['hw_2_d-grigorev']
) as dag:
    t1 = BashOperator(
        task_id='pwd_print',
        bash_command='pwd',
        dag=dag
    )


    def print_ds(ds, **kwargs):
        print(ds)
        return 'printing ds'


    t2 = PythonOperator(
            task_id='printing_ds',
            dag=dag,
            python_callable=print_ds
    )

t1 >> t2