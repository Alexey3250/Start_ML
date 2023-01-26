from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


with DAG(
        'hw_12_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 31),
        catchup=False,
        tags=['hw_12_n-anufriev']
) as dag:
    def variable():
        var_ = Variable.get('is_startml')
        print(var_)


    print_variable = PythonOperator(
        task_id='print_var',
        python_callable=variable)
