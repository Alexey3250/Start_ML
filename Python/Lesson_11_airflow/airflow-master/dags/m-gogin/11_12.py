from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        'hw_12_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_12_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 25),
        catchup=False,
        tags=['hw_12'],
) as dag:

    def print_var():
        from airflow.models import Variable
        var = Variable.get("is_startml")
        print(var)


    t1 = PythonOperator(
        task_id="11_12",
        python_callable=print_var
    )

    t1
