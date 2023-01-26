from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def ds_input(ds, **kwargs):
    print(ds)
    return ds


with DAG(
    'hw_3_v-kravtsova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    description = 'hw_2',
    start_date = datetime(2022, 12, 23),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False

) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """
    )
    t1 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
    )

t1