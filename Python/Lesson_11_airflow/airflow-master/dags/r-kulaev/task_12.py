from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_variable():
    from airflow.models import Variable
    result = Variable.get("is_startml")
    print(result)

with DAG(
        'hw_12_r-kulaev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_12_r-kulaev',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 29),
        catchup=False,
        tags=['hw_12_r-kulaev']
) as dag:
    t1 = PythonOperator(
        task_id="print_variable",
        python_callable=get_variable,
    )
