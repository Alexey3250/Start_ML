from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)  # timedelta из пакета datetime
}
with DAG('hw_10_n-eremenko',
         default_args = default_args,
         description ='hw_10_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_10_n-eremenko']) as dag:

    def take_xcom(ti):
        return "Airflow tracks everything"

    def give_xcom(ti):
        xcom_value = ti.xcom_pull(
            key="return value",
            task_ids="python_take_xcom")
        print(xcom_value)


    t1 = PythonOperator(task_id='python_take_xcom',
                        python_callable=take_xcom)

    t2 = PythonOperator(task_id='python_give_xcom',
                        python_callable=give_xcom)

    t1 >> t2
