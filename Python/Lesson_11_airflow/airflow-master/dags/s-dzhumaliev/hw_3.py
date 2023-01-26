from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'HW_3_s-dzhumaliev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id=f"echo_{i}",
            bash_command=f"echo {i}",
        )

        t.doc_md = f"""
            # Bash №{i}
            **Documentation** for `BashOperator` *echo_{i}*.
        """

    for i in range(20):
        t = PythonOperator(
            task_id=f'print_task_number_{i}',
            op_kwargs={'task_number': i},
            python_callable=print_task_number
        )

        t.doc_md = f"""
            # Python №{i}
            **Documentation** for `PythonOperator` *print_task_number*.
        """
