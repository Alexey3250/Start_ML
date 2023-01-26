from airflow.operators.bash import BashOperator
from textwrap import dedent
from datetime import datetime, timedelta

from airflow import DAG

with DAG(
    'task_5_aktanova_b',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

# Для каждого i в диапазоне от 0 до 5 не включительно
# распечатать значение ts и затем распечатать значение run_id

    template = dedent(
    '''\
        {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
    '''
)

    t1=BashOperator(
        task_id='templated_bash_command',
        bash_command=template
    )