from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable



with DAG(
    'example_branch_operator',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2023, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:

    def choose_branch():
        result = Variable.get("is_startml")
        if result == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"


    def task_true():
        return print("StartML is a starter course for ambitious people")


    def task_false():
        return print("Not a startML course, sorry")


    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
    )

    task2 = PythonOperator(
        task_id="startml_desc",
        python_callable=task_true)

    task3 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=task_false)

    branching >> [task2, task3]
