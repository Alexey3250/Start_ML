from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


with DAG(
        's-duzhak-2-task_8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['example'],
) as dag:

    def put_data(ti):
        ti.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
        )

    def get_data(ti):
        xcom_data = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids='python_xcom_push'
        )
        print(xcom_data)

    t1 = PythonOperator(
        task_id='python_xcom_push',
        python_callable=put_data
    )

    t2 = PythonOperator(
        task_id='python_xcom_pull',
        python_callable=get_data
    )

    t1 >> t2



