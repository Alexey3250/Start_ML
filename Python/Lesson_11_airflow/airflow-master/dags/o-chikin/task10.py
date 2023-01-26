from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'o-chikin_task10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='task10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['Oleg_Chikin_DAG']
) as dag:

    def xcom_push():
        return "Airflow tracks everything"

    def xcom_pull(ti):
        result = ti.xcom_pull(
            key='return_value',
            task_ids='xcom_push')
        print(result)

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2
