from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG \
            (
            "task_4_v_demareva",
            default_args={
                'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            },
            description="DAG for task #4",
            schedule_interval=timedelta(days=1),
            start_date=datetime(2022, 8, 21),
            catchup=False,
            tags=["task_4"]
        ) as dag:
    def print_task_num(task_number):
        print (f"task number is: {task_number}")

    for i in range(30):
        if i <= 10:
            bash_task = BashOperator(
                task_id = "BO_task_" + str(i),
                bash_command = f"echo {i}"
            )
        else:
            py_task = PythonOperator(
                task_id = "PY_task_" + str(i),
                python_callable = print_task_num,
                op_kwargs = {"task_number": i}
            )

    bash_task.doc_md = """

    #This is a documentation placed anywhere

    `code 1`

     ```code 2```

    *text1*

    **text2**

    _italic_

    """

    py_task.doc_md = """

    #This is a documentation placed anywhere

    `code 1`

    ```code 2```

    *text1*

    **text2**

    _italic_

    """

    bash_task >> py_task