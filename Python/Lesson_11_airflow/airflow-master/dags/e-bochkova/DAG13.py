from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


def set_course():
    from airflow.models import Variable
    course =  Variable.get("is_startml")
    if course == "True":
        return "startml_desc"
    return "not_startml_desc"


def print_is_startml():
    print("StartML is a starter course for ambitious people")


def print_is_not_startml():
    print("Not a startML course, sorry")


with DAG(
    'hw_13_e-bochkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description = 'A simple homework DAG',
    # Как часто запускать DAG
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup = False,
    # теги, способ помечать даги
    tags = ['homework13'],
) as dag:

    mock_begin = DummyOperator(
        task_id = 'the_mock_begin',
    )

    selection = BranchPythonOperator(
        task_id='choose_course',
        python_callable=set_course,
    )
    course1 = PythonOperator(
        task_id = "startml_desc",
        python_callable=print_is_startml
    )
    course2 = PythonOperator(
        task_id = "not_startml_desc",
        python_callable=print_is_not_startml
    )
    mock_end = DummyOperator(
        task_id = 'the_mock_end',
    )

    mock_begin >> selection >> [course1, course2] >> mock_end