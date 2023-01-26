from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

# В документации обязательно должны быть элементы кода (заключены в кавычки `code`),
# полужирный текст и текст курсивом, а также абзац (объявляется через решетку)


"""
Task 3 documentation
#### Task Documentation
    **Prints number** of iteration *in console* with command:
    `f'echo {i}'`
"""

ds_date = "{{ ds }}"
ts = "{{ ts }}"
run_id = "{{ run_id }}"

def print_task_num(task_number, ts, run_id):
    print(f'task number is: {task_number}, ts is: {ts}, run_id is: {run_id}')

with DAG(
        'hw_6_DAG_nivanova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 18),
        catchup=False,
        tags=['hw'],
) as dag:
    for i in range(30):
        if i < 10:
            echo_task = BashOperator(
                task_id='echo' + str(i),  # id, будет отображаться в интерфейсе
                bash_command="echo $NUMBER",  # какую bash команду выполнить в этом таске
                env={'NUMBER': str(i)}
            )
        else:
            python_task = PythonOperator(
                task_id='print_task_num' + str(i),
                python_callable=print_task_num,
                op_kwargs={'task_number': i, 'ts': ts, 'run_id': run_id}
            )

    dag.doc_md = __doc__

    echo_task.doc_md = dedent(
        """\
    #### Task Documentation
    Prints number of iteration *in console* with **command**:
    `f'echo {i}'`
    """
    )

    echo_task.doc_md = dedent(
        """\
    #### Task Documentation
    Prints number of iteration with *python* **command**:
    `print_task_num`
    """
    )

    # порядок выполнения
    echo_task >> python_task