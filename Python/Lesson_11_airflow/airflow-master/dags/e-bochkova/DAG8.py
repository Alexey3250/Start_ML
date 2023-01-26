from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_xcom(ti):
    xcom_sample = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='task_push',
    )
    print(xcom_sample)



with DAG(
    'hw_9_e-bochkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description = 'A simple homework DAG',
    # Как часто запускать DAG
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup = False,
    # теги, способ помечать даги
    tags = ['homework3'],
) as dag:

    t1 = PythonOperator(
        task_id="task_push",
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id="task_pull",
        python_callable=pull_xcom,
    )

    t1 >> t2
