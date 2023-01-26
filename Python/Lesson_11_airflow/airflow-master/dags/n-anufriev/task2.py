from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 27),
        catchup=False,
        tags=['hw_2_n-anufriev']
) as dag:
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd')


    def print_ds(ds):
        print(ds)
        return 'Ok'


    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds)

    t1 >> t2
