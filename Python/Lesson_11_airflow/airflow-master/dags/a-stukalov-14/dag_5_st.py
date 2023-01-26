from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'dag_6_stukalov',

    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags = ['st_6_ex'],
    start_date = datetime(2022,11,20)
) as dag:

    templated_command = dedent(
    """
    {% for i in range(5) %}
    echo "{{ ts }}"
    echo "{{ run_id }}"
    {% endfor %}
    """
    )  # поддерживается шаблонизация через Jinja
    # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating

    t1 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    )

    t1

