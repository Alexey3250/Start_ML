from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def extract_value():
    from airflow.models import Variable
    needed_value = Variable.get("is_startml")
    print(needed_value)


with DAG(
        dag_id="task12_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    secret_value = PythonOperator(
        task_id="secret_value",
        python_callable=extract_value
    )
