from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'chemelson_hw2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='chemelson_hw2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['chemelson_hw2']
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )


    def print_ds(ds, **kwargs):
        print(ds)
        return 'print_ds function return value'


    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )

    t1 >> t2
