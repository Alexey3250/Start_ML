from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'most_unique_name_off_all_time',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 9, 13),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['a-savelev-12']
) as dag:

    tmp_command = dedent(
        """
        {% for i in range(5)%}
            echo "{{ts}}"
            echo "{{run_id}}
        {% endfor %}
        """
    )
    t1 = BashOperator(
        task_id='tmp',
        bash_command=tmp_command
    )
    t1
