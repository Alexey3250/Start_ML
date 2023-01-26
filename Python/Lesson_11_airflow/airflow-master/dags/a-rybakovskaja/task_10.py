from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'XCom_test_2',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2023, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:
    def xcom_push():
        return "Airflow tracks everything"

    def xcom_pull(ti):
        value = ti.xcom_pull(
            key="return_value",
            task_ids="xcom_push")
        return print(value)

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push)

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull)

    t1 >> t2
