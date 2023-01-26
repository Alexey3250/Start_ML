from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def print_context(ds, **kwargs):
    task_number = kwargs['task_number']
    print(f"task number is: {task_number}")
    return 'Whatever you return gets printed in the logs'


with DAG(
        '3_m-taljanova',
        start_date=datetime(2022, 12, 23),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=5),
        default_args=default_args,
        catchup=False

) as dag:

    for i in range(10):
        task_bash = BashOperator(
            task_id='print_date_' + str(i),
            bash_command=f"echo {i}",
        )


        task_bash.doc_md = dedent(
   	"""\
   	#### BashOperator Documentation
    	You can print a row of **10** numbers by using `echo {i}`

    	"""
   	) 





    for i in range(11,30):
        task_python = PythonOperator(
            task_id='print_the_context_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )


        task_python.doc_md = dedent(
   	"""\
   	#### PythonOperator Documentation
    	You can print a row of **10** numbers by using f-string `f"task number is: {num}"`

    	"""
   	) 

