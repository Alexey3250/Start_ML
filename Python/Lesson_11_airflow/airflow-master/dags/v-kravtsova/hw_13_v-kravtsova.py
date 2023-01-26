from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta


def det_course(is_startml):
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"

def variant1():
    print("StartML is a starter course for ambitious people")

def variant2():
    print("Not a startML course, sorry")


with DAG(
    'hw_12_v-kravtsova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    description = 'hw_12',
    start_date = datetime(2022, 12, 23),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False

) as dag:
    t1 = PythonOperator(
        task_id="startml_desc",
        python_callable=variant1
    )

    t2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=variant2
    )

    t3 = BranchPythonOperator(
        task_id="determine_course",
        python_callable = det_course

    )




t3>>[t1,t2]