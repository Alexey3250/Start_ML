from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator


with DAG(
    '11_7_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],
) as dag:

    def twenty_tasks(ts, run_id, **kwargs):
        print("ts")
        print("run_id")

    for i in range(11, 31):
        task = PythonOperator(
            task_id='task_' + str(i),
            python_callable=twenty_tasks,
            op_kwargs={'task_number': i},
        )
