from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'o-gurylev_task_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
            description='First DAG task_2',
            schedule_interval=timedelta(days=1),
            start_date=datetime(2022, 1, 1),
            catchup=False,
            tags=['task_2'],
) as dag:
    print_pwd = BashOperator(
        task_id='show_pwd',
        bash_command='pwd',
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'


    print_ds = PythonOperator(
        task_id='print_ds',
        python_callable=print_context,
    )

    print_pwd >> print_ds
