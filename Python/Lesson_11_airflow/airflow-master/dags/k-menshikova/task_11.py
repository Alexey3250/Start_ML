from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_11_k-menshikova',
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
    tags=['hw_11_k-menshikova'],
) as dag:

    start = DummyOperator(task_id="before_branching")
    end = DummyOperator(task_id="after_branching")

    print_var = PythonOperator(
        task_id="print_var",
        python_callable=lambda: print(Variable.get("is_startml"))
    )

    start >> print_var >> end
