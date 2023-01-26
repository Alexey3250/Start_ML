from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    'den_sokolov_step_10',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 7, 19), 
    catchup=False,
    tags=['den-sokolov'],
) as dag:

        

    def get_top_user():
        
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor
    
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('''
                            select 
                                user_id 
                                , count( action ) 
                            from feed_action
                            where action = 'like'
                            group by 
                                user_id
                            order by 
                                count( action ) desc
                            limit 1
                        ''')
                results = cursor.fetchone()
        
        return results
   
    t1 = PythonOperator(
        task_id='get_user_from_feed_action',
        python_callable=get_top_user
    )


    t1