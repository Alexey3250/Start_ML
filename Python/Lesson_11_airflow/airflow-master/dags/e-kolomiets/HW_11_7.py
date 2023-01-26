from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'kolomiets_11_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lerning DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 21),
        catchup=False,
) as dag:
    def get_data(ts, run_id, **kwargs):
        print(f'ts: {ts}, run_id: {run_id}')


    for i in range(20):
        t1 = PythonOperator(
            task_id=f"task_number_{i}",
            python_callable=get_data,
            op_kwargs={'task_number': i}
        )

    t1
