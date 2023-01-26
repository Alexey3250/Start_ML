from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


with DAG(
        'hw_7_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_7_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['hw_6'],
) as dag:

    def get_task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(30):
        t1 = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )

    t1
