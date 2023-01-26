from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"

with DAG(
    'hw_4_j_ponomareva_01',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG3_jp',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['j_pon_03'],
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f"echo_task_number_{i}",
                bash_command=f"echo {i}"
            )
        else:
            t2 = PythonOperator(
                task_id=f"print_task_number_{i}",
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )
            t1.doc_md = dedent(
                """\
                #### Bash Operator
                It is used to execute chance command `if i < 10:`
                The variable **i** changes its values *from 0 to 9*
                #### Bash Operator
                It is used to execute bash command `echo {i}`
                The variable __i__ changes its values _from 0 to 9_
                
                """
            )

            t2.doc_md = dedent(
                """
                #### Print Operator
                There is a print of the type `print(i)` 
                The variable **i** changes its values *from 0 to 19*
                ### Print Operator
                There is a print of the type `print()` 
                The variable __i__ changes its values _from 0 to 19_
                """
            )

            t1 >> t2



from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_3_j_ponomareva_01',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG3_jp',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['j_pon_03'],
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f"echo_task_number_{i}",
                bash_command=f"echo {i}"
            )
        else:
            t2 = PythonOperator(
                task_id=f"print_task_number_{i}",
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

            t1 >> t2





