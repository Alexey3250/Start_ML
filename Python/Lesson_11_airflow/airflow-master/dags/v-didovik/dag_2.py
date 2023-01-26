from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_2_v-didovik',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_2_v-didovik',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_2_v-didovik'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='pwd',
    )

    def print_ds(ds, **kwargs):
        print(ds)
        return 'Done'

    t2 = PythonOperator(
        task_id='print_the_ds',
        python_callable=print_ds,
    )


    t1 >> t2

