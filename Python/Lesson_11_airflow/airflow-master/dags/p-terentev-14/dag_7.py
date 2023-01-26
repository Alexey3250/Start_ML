from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        "hw_7_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['DAG_6']
) as dag:
    def print_task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)


    for i in range(1, 10):
        t2 = PythonOperator(
            task_id='python_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )