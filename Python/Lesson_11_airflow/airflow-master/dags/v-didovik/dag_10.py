from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_10_v-didovik',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_10_v-didovik',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_10_v-didovik'],
) as dag:

    def return_msg():
        return "Airflow tracks everything"

    def pull_xcom(ti):
        msg = ti.xcom_pull(
            task_ids='push_xcom',
            key='return_value'
        )
        print(msg)

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=return_msg,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2
