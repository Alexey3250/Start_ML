from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'e-kolomiets_lesson11_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lerning DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 21),
        catchup=False,
) as dag:
    t1 = BashOperator(
        task_id='pwd_call',
        bash_command='pwd'
    )


    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='printing_ds',
        python_callable=print_ds,
    )

    t1 >> t2
