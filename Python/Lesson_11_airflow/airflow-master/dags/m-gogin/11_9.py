from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
        'hw_9_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_9_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 25),
        catchup=False,
        tags=['hw_9'],
) as dag:

    def push_xcom_test(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def pull_xcom_test(ti):
        xcom_test = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(xcom_test)


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom_test
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_test,
    )

    t1 >> t2


