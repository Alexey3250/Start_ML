# Импортируем необходимые модули и библиотеки
import os
from catboost import CatBoostClassifier, Pool, CatBoost
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from typing import List
from fastapi import FastAPI, Depends, HTTPException, Query 
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List
import logging
import pydantic

'''
ФУНКЦИИ ПО ЗАГРУЗКЕ МОДЕЛЕЙ
'''
# Проверка если код выполняется в лмс, или локально
def get_model_path(path: str) -> str:
    """Просьба не менять этот код"""
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        #MODEL_PATH = '/workdir/user_input/model'
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

class CatBoostWrapper(CatBoost):
    def predict_proba(self, X):
        return self.predict(X, prediction_type='Probability')

# Загрузка модели
def load_models():
    model_path = get_model_path("catboost_model_1.cbm")
    model = CatBoostWrapper()
    model.load_model(model_path)
    return model


'''
Получение данных из базы данных
'''

# Определяем функцию для получения данных из базы данных PostgreSQL
def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

def load_features() -> pd.DataFrame:
    query = "a-efimik_features_lesson_22_4"
    return batch_load_sql(query)


'''
# Определяем класс Post для работы с таблицей базы данных post
class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)
    
'''
'''
class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True
'''
        
class Post(BaseModel):
    id: int
    text: str
    topic: str

class PostList(BaseModel):
    posts: List[Post]

# Определяем функцию для получения сессии базы данных
def get_db():
    with SessionLocal() as db:
        return db




# Определяем переменные для подключения к базе данных
SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
print("Подключение к базе данных PostgreSQL установлено")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
print("Сессия базы данных установлена")

Base = declarative_base()
print("Базовый класс для определения таблицы базы данных установлен")


model = load_models()
print("Модель загружена")

features = load_features()
print("Данные загружены")


def caching_predictions(feed_data, model):
    # Get a list of unique users
    unique_users = feed_data['user_id'].unique()

    # Create a group ID based on the 'user_id'
    group_id_dict = {user_id: idx for idx, user_id in enumerate(unique_users)}
    feed_data['group_id'] = feed_data['user_id'].map(group_id_dict)

    feed_data_sorted = feed_data.sort_values(by='group_id')
    data_pool = Pool(feed_data_sorted.drop(columns=['user_id']), group_id=feed_data_sorted['group_id'])
    y_pred_proba = model.predict_proba(data_pool)[:, 1]
    feed_data['pred_proba'] = y_pred_proba
    top_5_posts = feed_data.groupby('user_id').apply(lambda x: x.nlargest(5, 'pred_proba')['post_id'])
    top_5_posts_dict = top_5_posts.reset_index().groupby('user_id')['post_id'].apply(list).to_dict()
    
    return top_5_posts_dict


top_posts_dict = caching_predictions(features, model)
print("Предсказания сделаны")

'''
top_posts_dict = {
    200: [1630, 1881, 1864, 5593, 6661],
    201: [1336, 821, 3371, 5514, 4150],
    202: [5208, 2418, 7014, 4730, 4231],
    203: [4541, 1795, 2062, 5490, 5423],
    # ... (other data goes here)
}
'''

'''
FAST API код
'''

class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True # ORM Это объектно-реляционное отображение (англ. Object-Relational Mapping, ORM) — технология программирования, которая связывает базы данных с концепциями объектно-ориентированных языков программирования, создавая «виртуальную объектную базу данных».

app = FastAPI()

@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
        id: int, 
        time: datetime, 
        limit: int = 5) -> List[PostGet]:
    
    # Retrieve the top posts
    post_ids = top_posts_dict.get(id)
    
    if not post_ids:
        raise HTTPException(status_code=404, detail="User not found")

    # Retrieve the posts from the database
    # You may need to implement this part according to your database setup
    query = "SELECT * FROM post_text_df WHERE post_id IN ({})".format(", ".join(map(str, post_ids[:limit])))
    df = pd.read_sql(query, engine)

    # Add a print statement to inspect the DataFrame
    print(df)
    print("Посты получены")
    # Add another print statement to inspect the list of records
    records = df.to_dict('records')
    print(records)
    print("Посты преобразованы в словарь")

    posts = []
    for rec in records:
        rec["id"] = rec.pop("post_id")  # change "post_id" to "id"
        try:
            posts.append(PostGet(**rec))
        except pydantic.error_wrappers.ValidationError as e:
            print(f"Validation error for record {rec}: {e}")


    return posts