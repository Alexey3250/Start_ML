from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'hw_9_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 31),
        catchup=False,
        tags=['hw_9_n-anufriev']
) as dag:

    def xcom_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test')

    def xcom_pull(ti):
        pull = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='xcom_pull')
        print(pull)


    pusher = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_push)

    puller = PythonOperator(
        task_id='result',
        python_callable=xcom_pull)

    pusher >> puller
