from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



def xcom_test(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def xcom_print(ti):
    test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='test_push'
    )
    print(test)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'xcom_dag_galevskii',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'test_push',
        python_callable=xcom_test
    )
    t2 = PythonOperator(
        task_id = 'test_pull',
        python_callable=xcom_print
    )

    t1 >> t2