from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_variable():
    from airflow.models import Variable
    result = Variable.get("is_startml")
    print(result)

with DAG(
        'chemelson_hw12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='chemelson_hw12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['chemelson_hw12']
) as dag:
    t1 = PythonOperator(
        task_id="print_is_startml",
        python_callable=get_variable,
    )