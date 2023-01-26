from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'a-shagvaleev_ex2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 6, 19),
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = "bash_task_"+str(i),
            bash_command = "echo $NUMBER",
            env = {"NUMBER": i}
        )
        t1.doc_md = dedent(
            """
            # абзац для bash
            
            _текст курсивом_
            *ещё один текст курсивом*
            **полужирный текст**
            __ещё полужирный текст__
            `x = 5`
            """
        )


    def ds_func(task_number, ts, run_id):
        """Simple example for PythonOperator"""
        print(ts, run_id)
        return f"task number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id = "python_task_"+str(i),
            python_callable = ds_func,
            op_kwargs = {"task_number": i},
        )
        t2.doc_md = dedent(
            """
            # абзац для python
    
            _текст курсивом_
            *ещё один текст курсивом*
            **полужирный текст**
            __ещё полужирный текст__
            `x = 5`
            """
        )

    t1 >> t2

    # sdfsdfsd