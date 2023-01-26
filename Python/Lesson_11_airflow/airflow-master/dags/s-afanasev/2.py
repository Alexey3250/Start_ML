from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_8_s-afanasev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
) as dag:
    def print_task_number(ts, run_id, **kwargs):
        """Распечатать номер задачи"""
        print(ts)
        print(run_id)
        print(f"task number is: {kwargs}")


    for i in range(10):
        t1 = BashOperator(
            task_id="print_number_" + str(i),
            env={'NUMBER': i},
            bash_command="echo $NUMBER",
        )

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )

    t1.doc_md = """
        ### Task description
        Printing task number via **BashOperator** using `echo`.
        10 cycles through *for* syntax end env 
        
        """

t1 >> t2
