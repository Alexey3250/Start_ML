from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        "hw_10_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['DAG_10']
) as dag:
    def return_string():
        return "Airflow tracks everything"

    def pull_xcom(ti):
        testing_pull = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(testing_pull)

    a1 = PythonOperator(
        task_id='push_xcom',
        python_callable=return_string,
    )
    a2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )
    a1 >> a2