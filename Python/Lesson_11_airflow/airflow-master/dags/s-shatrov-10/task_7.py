from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'shatrov_task_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="task_2",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 27),
) as dag:

    def print_task_number(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)

    for i in range(30):
        
        if i < 10:
            t1 = BashOperator(
                task_id=f"bash_{i}",
                env={"NUMBER": i},
                bash_command="echo $NUMBER"
            )
        else:
            t2 = PythonOperator(
                task_id=f"python_{i}",
                python_callable=print_task_number,
                op_kwargs={"task_number": i}
            )

    t1 >> t2