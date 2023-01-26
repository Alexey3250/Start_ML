"""Homework_13 script"""

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


def select_task():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")
    return "startml_desc" if is_startml == 'True' else "not_startml_desc"

def not_startml_desc():
    print("Not a startML course, sorry")

def startml_desc():
    print("StartML is a starter course for ambitious people")


with DAG(
        'hw_13_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_13']
) as dag:

    before_branching = DummyOperator(
        task_id="before_branching"
        #nothing called
    )

    before_branching.doc_md = dedent(
        """
        ## Перед `BranchPythonOperator` можете поставить `DummyOperator` -
        он ничего не делает, но зато задает красивую "стартовую точку" на графе.
        """
    )

    branch = BranchPythonOperator(
        task_id="determine_course",
        python_callable=select_task,
    )

    branch.doc_md = dedent(
        """
        ## `BranchingOperator` - это оператор, который
        по некоторому условию определяет, в какое ответвление
        пойдет выполнение DAG. Один из способов определить это
        "некоторое условие" - это задать python функцию,
        которая будет возвращать `task_id`,
        куда надо перейти после ветвления.
        """
    )

    not_good_way = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc,
    )

    not_good_way.doc_md = dedent(
        """
        ## иначе перейти в таску с `task_id="not_startml_desc"`.
        """
    )

    good_way = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_desc,
    )

    good_way.doc_md = dedent(
        """
        ## Если значение Variable `is_startml` равно `"True"`,
        то перейти в таску с `task_id="startml_desc"`
        """
    )
    after_branching = DummyOperator(
        task_id="after_branching",
        #nothing called
    )

    after_branching.doc_md = dedent(
        """
        ## Точно так же можете поставить `DummyOperator` в конце DAG.
        """
    )

    before_branching >> branch >> [not_good_way, good_way] >> after_branching