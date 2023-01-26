from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'task_9_v_demareva',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 9)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 21),
        catchup=False,
        tags=["task_9"]
) as dag:

    def set_variable(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )
        print("variable set")

    def get_variable(ti):
        ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='t9_python_1'
        )
        print("variable get")

    t1 = PythonOperator(
        task_id="t9_python_1",
        python_callable=set_variable,
    )

    t2 = PythonOperator(
        task_id="t9_python_2",
        python_callable=get_variable,
    )

    t1 >> t2