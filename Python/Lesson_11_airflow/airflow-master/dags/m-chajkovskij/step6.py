from datetime import datetime, timedelta
from textwrap import dedent
# To declare a DAG the DAG class of the airflow must be imported
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "task_11.3",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes =5),
    },
    description='m-chajkovskij_11.3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['homework']
)as dag:
    for i in range(10):
        bash_t = BashOperator(task_id=f'bash_task_{i}', bash_command=f'echo $NUMBER', env={'NUMBER': i} )
        bash_t.doc_md = """
        ## Bash task documentation in the form of *markdown*. 
        The **task** is created using `BashOperator` from the module *airflow*
        """

