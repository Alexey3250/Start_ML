from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def save(ti):
    return ti.xcom_push(key="sample_xcom_key", value="xcom_test",)
def load(ti):
    return print(ti.xcom_pull(key="sample_xcom_key"))

with DAG(
        'norgello-9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='first task in lesson №11',
        schedule_interval=timedelta(days=3650),
        start_date=datetime(2022, 10, 20),
        catchup=False
) as dag:
        m1 = PythonOperator(
            task_id='bash_command',
            python_callable=save)
        m2 = PythonOperator(
            task_id='number_tasks_on_python',
            python_callable=load
        )
m1>>m2