from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'dag8_g-volosnyh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag8',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    def airflow_tracks():
        return "Airflow tracks everything"

    t1 = PythonOperator(
        task_id=f'task_push',
        python_callable=airflow_tracks,
    )

    def test_pull(ti):
        val = ti.xcom_pull(
            key='return_value',
            task_ids='task_push'
        )
        print(val)

    t2 = PythonOperator(
        task_id=f'task_pull',
        python_callable=test_pull,
    )

    t1 >> t2
