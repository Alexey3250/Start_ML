from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def return_ts(ts):
    return ts


def return_run_id(run_id):
    return run_id


with DAG(
        dag_id="task5_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    for value in range(0, 5):
        bash_tasks = BashOperator(
            task_id=f"task_{value}",
            bash_command=f"Для каждого {value} в диапазоне от 0 до 5 не включительно распечатать значение {return_ts} и затем распечатать значение {return_run_id}")
