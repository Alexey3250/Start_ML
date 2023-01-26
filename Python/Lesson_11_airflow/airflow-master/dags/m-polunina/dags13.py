from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


with DAG('polunina_13',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    description='A unit 13',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['A unit 13'],
) as dag:

    def decide_which_path():
        from airflow.models import Variable
        is_startml = Variable.get("is_startml")
        if is_startml == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"
   
    t1 = DummyOperator(task_id="before_branching")
    
    branch_task = BranchPythonOperator(
    task_id='run_this_first',
    python_callable=decide_which_path,
    #trigger_rule="all_done",
    dag=dag)

    def startml_d():
       print("StartML is a starter course for ambitious people")

    t_startml_desc = PythonOperator(task_id = 'startml_desc', python_callable = startml_d)

    def not_startml_d():
       print("Not a startML course, sorry")

    t_not_startml_desc = PythonOperator(task_id = 'not_startml_desc', python_callable = not_startml_d)

    branch_task>>[t_startml_desc, t_not_startml_desc]





