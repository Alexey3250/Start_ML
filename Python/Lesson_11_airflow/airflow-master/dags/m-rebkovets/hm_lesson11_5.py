from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator


with DAG(
    '11_5_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],
) as dag:

    templated_command=dedent(
        """
        {%for i in range(5)%}
            echo "{{  ts }}"
            echo "{{ run_id }}"
        {% endfor %}
    """)

    task = BashOperator(
            task_id='templated',
            bash_command=templated_command,
        )


    task.doc_md = dedent(
    """
    ## text
    textextext
    **text**, *text*, `code`
    """
    )