from datetime import datetime, timedelta
from textwrap import dedent

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

    t1.doc_md = dedent(
        '''\
        # test documentation
        `def print_ds(ds):
            print(ds)`
        _italic_
        __bold__
        '''
    )

    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='printing_ds',
        python_callable=print_ds,
    )

    t2.doc_md = dedent(
        '''\
        # test documentation
        `def print_ds(ds):
            print(ds)`
        _italic_
        __bold__
        '''
    )

    t1 >> t2
