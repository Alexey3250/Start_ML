from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import timedelta,datetime

def print_context(ds, **kwargs):
        print(ds)
        print(kwargs)

        return 'Whatever you return gets printed in the logs'

def print_task_number(task_number,ts,run_id):
    #print(kwargs.ts)
    #print(kwargs.run_id)
    print('ts - '+ts)
    print('run_id - '+run_id)
    print(f"task number is: {task_number}")
    return "task number printedq"

with DAG(
    'hw_3_v-hramenkov-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'description':'напишите любое описание',
        'schedule_interval':timedelta(days=22),
        'start_date':datetime(2022, 10, 28),
        'catchup':False,
        'tags':['любой тэг, чтобы искать свой даг на airflow'],
}) as dag:
    
    for i in range(30):
        if i < 10:
            task_1 = BashOperator(
                task_id=f"echo_task_number_{i}",
                bash_command=f"echo {i}"
            )
        else:
            task_2 = PythonOperator(
                task_id='print_task_number_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

task_1 >> task_2#,'ts':{{ts}},'run_id':{{run_id}}