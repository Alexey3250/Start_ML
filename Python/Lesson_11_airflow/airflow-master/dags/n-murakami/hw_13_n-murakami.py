from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

def choose_path():
    from airflow.models import Variable
    if Variable.get("is_startml")=="True":
        return "startml_desc"
    else:
        return "not_startml_desc"
def print_if_true():
    return "StartML is a starter course for ambitious people"

def print_if_false():
    return "Not a startML course, sorry"

with DAG(
        'hw_13_n-murakami',
        default_args={
          'depends_on_past': False,
          'retries': 3,
          'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_13_n-murakami']
) as dag:
    decision = BranchPythonOperator(
        task_id="decide_path",
        python_callable=choose_path
    )
    dummy_start= DummyOperator(
        task_id="start_graph"
    )
    dummy_end= DummyOperator(
        task_id="end_graph"
    )
    if_true = PythonOperator(
        task_id="startml_desc",
        python_callable=print_if_true
    )
    if_false = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_if_false
    )
    dummy_start >> decision >> [if_true, if_false] >> dummy_end



