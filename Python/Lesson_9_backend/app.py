from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

# connect to database
conn = psycopg2.connect("postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml")
cursor = conn.cursor()

# create FastAPI app
app = FastAPI()

@app.get("/user/{id}")
def get_user(id: int):
    cursor.execute(
        f"""
        SELECT *
        FROM "user"
        WHERE id = {id}
        """
        )
    user = cursor.fetchone()
    if user:
        return user
    else:
        raise HTTPException(status_code=404, detail="User not found")