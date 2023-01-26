from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        "hw_12_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['DAG_12']
) as dag:
    def print_var():
        from airflow.models import Variable
        print(Variable.get("is_startml"))


    a1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_var
    )

    a1