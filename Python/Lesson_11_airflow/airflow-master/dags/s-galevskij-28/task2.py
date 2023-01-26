from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'number_2',
    default_args={
        'depends_on_past': False,
        'email':['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Bash + Python operators',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['example'],
) as dag:
    t1 = BashOperator(
        task_id='bash',
        bash_command='pwd'
    )

    def logic_time(ds, **kwargs):
        print(ds)
        return('first DAG!')

    t2 = PythonOperator(
        task_id='python',
        python_callable=logic_time
    )

    t1 >> t2