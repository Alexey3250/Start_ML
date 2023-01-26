from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'fifth',
        default_args={
            'depends_on_past': False,
            'email': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='DAG for Task 05',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 23),
) as dag:

    for i in range(10):
        t_0_10 = BashOperator(
            task_id='print_smth_' + str(i),
            bash_command="echo $NUMBER",
            dag=dag,
            env={"NUMBER": i},
        )
