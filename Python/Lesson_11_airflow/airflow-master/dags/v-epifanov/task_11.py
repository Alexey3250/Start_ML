from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def get_user():
    sql_str = """
        select user_id, count(*)
          from feed_action 
          where action = 'like'
          group by user_id
          order by count(*) desc 
          limit 1;
        """

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_str)
            result = cursor.fetchall()

    return result


with DAG(
    'task_11_vepifanov',
    description='vepifanov, задание 11.11',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['vepifanov'],
) as dag:

    t_1 = PythonOperator(
        task_id = f"get_user",
        python_callable=get_user,
    )

    t_1

