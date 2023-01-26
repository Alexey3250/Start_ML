from cgitb import reset
import psycopg2
import psycopg2.extras
with psycopg2.connect(
    database="startml",
    user="robot-startml-ro",
    password="pheiph0hahj1Vaif",
    host="postgres.lab.karpov.courses",
    port=6432
)as conn:
    with conn.cursor() as cursor:
        cursor.execute( """select user_id, count(action)
                    from feed_action
                    where action = 'like'
                    group by user_id
                    order by count(action) desc
                    limit 1""")
        result = cursor.fetchone()     

answer = {
    'user_id' : result[0],
    'count' : result[1]
}
print(answer)
