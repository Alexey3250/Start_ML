from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'hw_10_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 31),
        catchup=False,
        tags=['hw_10_n-anufriev']
) as dag:

    def xcom_push(ti):
        return "Airflow tracks everything"

    def xcom_pull(ti):
        pull = ti.xcom_pull(
            key='return_value',
            task_ids='xcom_pull')
        print(pull)


    pusher = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_push)

    puller = PythonOperator(
        task_id='result',
        python_callable=xcom_pull)

    pusher >> puller
