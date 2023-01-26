from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'murad_satabaev_first_task',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='murad_satabaev_first_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 10),
        catchup=False,
        tags=['murad_tag'],
) as dag:
    t1 = BashOperator(
        task_id='show_directory_in_bash_operator',
        bash_command='pwd'
    )

    def print_date_ds(ds):
        print(ds)
        print("this is murad_satabaev dag and task, this one is done in python operator")

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_date_ds,
    )
    t1 >> t2
