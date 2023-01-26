from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent



with DAG(
    'firstdag',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
},
        description='HW 3 MaximovS',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7,17),
        catchup=False,
        tags=['maximov'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id="print_directory_"+str(i),
            bash_command=f"echo $NUMBER",
            env={'NUMBER': i}
        )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **текст**
    """
    )


    def print_context(ts,run_id, **kwargs):
        print(kwargs)
        print(ts)
        print(run_id)
        return 'Whatever you return gets printed in the logs'
        # я люблю питон
    for i in range(20):
        t2 = PythonOperator(
            task_id="lala_" + str(i),
            python_callable=print_context,
            op_kwargs={"ts: i", "run_id: i"}
            )
    t1 >> t2