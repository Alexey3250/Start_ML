from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'murad_satabaev_eighth_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='murad_satabaev_sixth_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 10),
        catchup=False,
        tags=['murad_tag'],
) as dag:
    def push_xcom(ti, key, value):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def print_xcom(ti, task_ids, key):
        print(ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        ))


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=print_xcom,
    )

    t1 >> t2
