from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

"""
Test documentation
"""

with DAG(
    'hw9_bm',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw9_bm DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['BH_hw9'],
) as dag:

    def xcom_test(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )
        print('done')

    t1 = PythonOperator(
    task_id = 'testing_xcom_dag',
    python_callable = xcom_test
    )

    
    def retrieve_xcom(ti):
        print(ti.xcom_pull(key="sample_xcom_key", task_ids="testing_xcom_dag"))

    t2 = PythonOperator(
        task_id = 'testing_geting_xcom',
        python_callable = retrieve_xcom
    )
    
    t1 >> t2   