from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'task_12_v_demareva',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 12)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 21),
        catchup=False,
        tags = ['task12']
) as dag:

    def get_variable():
        from airflow.models import Variable

        print(Variable.get("is_startml"))

    t1 = PythonOperator(
        task_id="t12_python_1",
        python_callable=get_variable,
    )

    t1