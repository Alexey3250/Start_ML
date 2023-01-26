from datetime import datetime, timedelta
from textwrap import dedent

from click import echo

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'tutorial', 
    default_args={
        'depends_on_past': False,
        'email': ****,
        'email_on_failure': ***,
        'email_on_retry': ***,
        'retries': 1, 
        'retry_delay': timedelta(minutes=5)
    },
    description='tutorial DAG',
    schedule_interval=timedelta(days=1)
    start_time=datatime(2022, 1, 1),
    catchup=False,
    tags=['example']
) as dag:
    t1 = BashOperator(
        task_id = 'print_date',
        bash_command = 'date',
    )
    t2 = BashOperator(
        task_id = 'sleep',
        depends_on_past = False,
        bash_command = 'sleep 5',
        retries = 3,
    )
    t1.doc_md= dedent(
        """
        fdfesfsfsef
        """
    )
    dag.doc_md=__doc__

    templated_command = dedent(
    """
    {%for i in range(5)%}
        echo "{{ds}}"
        echo "{{macros.ds_add(ds, 7)}}"
    {% endfor %}
    """)

    t3 = BashOperator(
        task_id='templated'
        depends_on_past=False
        bash_command=templated_command,
    )

    t1 >> t2