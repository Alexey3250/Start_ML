from fastapi import FastAPI, Depends, HTTPException
import psycopg2
from pydantic import BaseModel

# создадим приложение
app = FastAPI()

# опишем модель данных, которую будем возвращать
class PostResponse(BaseModel):
    id: int
    text: str
    topic: str

    # включим orm_mode, чтобы можно было передавать данные в виде словаря
    class Config:
        orm_mode = True

# функция для подключения к базе данных
def get_db():
    conn = psycopg2.connect(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml",
    )
    return conn

# функция для получения списка постов
@app.get("/post/{id}", response_model=PostResponse)
def post_info(id: int, db = Depends(get_db)) -> PostResponse:
    
    # получим курсор для работы с базой данных
    cursor = db.cursor()
    
    # выполним запрос
    cursor.execute(
             f"""SELECT id, text, topic
                 FROM post
                 WHERE id = {id}
              """)
    
    # получим результат
    post = cursor.fetchone()
    
    # если пост не найден, то вернем ошибку
    if not post:
        raise HTTPException(status_code=404, detail="post not found")
    
    # преобразуем результат в словарь
    post = dict(zip(['id', 'text', 'topic'], post))
    
    # вернем результат
    # **post - распаковка словаря
    return PostResponse(**post)


