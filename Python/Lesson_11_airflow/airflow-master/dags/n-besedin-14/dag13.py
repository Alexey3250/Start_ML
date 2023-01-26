from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator

with DAG(
        'hw_13_n_besedin_14',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='besedin_lesson13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 1),
        catchup=False,
        tags=['hw_13_n_besedin_14']
) as dag:
    def return_task_id():
        var_ = Variable.get('is_startml')
        if var_ == 'True':
            task_id = "startml_desc"
        else:
            task_id = "not_startml_desc"
        return task_id

    def startml():
        print("StartML is a starter course for ambitious people")

    def not_startml():
        print("Not a startML course, sorry")

    difficult_transition = BranchPythonOperator(
        task_id='determine_course',
        python_callable=return_task_id)

    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml)

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml)

    difficult_transition >> [t1, t2]
