from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'task_10_v_demareva',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 10)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 21),
        catchup=False,
        tags=["task_10"]
) as dag:

    def set_variable():
        return "Airflow tracks everything"

    def get_variable(ti):
        ti.xcom_pull(
            key='return_value',
            task_ids='t10_python_1'
        )
        print("variable get")

    t1 = PythonOperator(
        task_id="t10_python_1",
        python_callable=set_variable,
    )

    t2 = PythonOperator(
        task_id="t10_python_2",
        python_callable=get_variable,
    )

    t1 >> t2