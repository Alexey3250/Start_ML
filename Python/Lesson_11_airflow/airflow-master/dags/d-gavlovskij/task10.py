from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task10_d-gavlovskij',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='somedag',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 10, 31),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['gavlique']
) as dag:
    def pushing():
        return "Airflow tracks everything"

    def pulling(ti):  # переменная ti - task instance - уже вшита в оператор
        print(ti.xcom_pull(task_ids='xcom_push'))

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=pushing,
        dag=dag
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=pulling,
        dag=dag
    )

    t1 >> t2
