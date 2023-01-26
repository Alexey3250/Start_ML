from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def connect_ds():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    import psycopg2
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
      with conn.cursor() as cursor:
        cursor.execute(
        """ 
        SELECT user_id, COUNT(action)
        FROM feed_action
        WHERE action = 'like'
        GROUP BY user_id
        ORDER BY count DESC
        LIMIT 1
        """
        )
        results = cursor.fetchone()
        print(results)
        return results


with DAG(
    'hw_11_v-kravtsova',
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
    t1 = PythonOperator(
        task_id='connect_ds',
        python_callable=connect_ds
    )


t1