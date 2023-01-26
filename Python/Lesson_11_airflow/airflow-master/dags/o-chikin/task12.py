from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'o-chikin_task12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='task12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['Oleg_Chikin_DAG']
) as dag:

    def get_variable():
        from airflow.models import Variable
        variable = Variable.get("is_startml")
        print(variable)

    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable,
    )
