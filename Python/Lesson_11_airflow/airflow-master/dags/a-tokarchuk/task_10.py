
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    'my_dag_task_3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    def implicit_xcom():
        return 'Airflow tracks everything'

    def get_implicit_xcom(ti):
        result = ti.xcom_pull(
            key='return_value',
            task_ids='push_to_xcom_implicit'
        )
        print(result)


    t1 = PythonOperator(
        task_id = 'push_to_xcom_implicit',
        python_callable=implicit_xcom
    )

    t2 = PythonOperator(
        task_id = 'pull_from_xcom_implicit',
        python_callable=get_implicit_xcom
        )

    t1 >> t2

