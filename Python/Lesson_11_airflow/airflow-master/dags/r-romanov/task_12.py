from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.models import Variable


with DAG(
        'hw_12_r-romanov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),

        catchup=False,
        # теги, способ помечать даги
        tags=['rm_romanov'],
) as dag:

    def print_context(is_startml):
        print(is_startml)


    is_startml = Variable.get("is_startml")
    t2 = PythonOperator(
        task_id='id_p_12',
        python_callable=print_context,
        op_kwargs={'is_startml': is_startml}
    )

