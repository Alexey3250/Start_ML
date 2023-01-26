from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG('hw_3_o_bulaeva_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries':1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'Second DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 7, 24),
        catchup = False) as dag:

    for i in range(10):
    	bash_task = BashOperator(
    	task_id = "bash_task_number_" + str(i),
    	bash_command = "echo $NUMBER",
        env={"NUMBER": i}, 
    	)
