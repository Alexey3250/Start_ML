from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'dm-kuznetsov_hw_2',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='dm-kuznetsov_hw_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['task_1'],
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
    )

    t1.doc_md = dedent(
        """ Print pwd """
    )

    def print_ds(ds):
        print(ds)
        print('Nice!')

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t1 >> t2