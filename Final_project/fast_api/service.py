# Импортируем необходимые модули и библиотеки
import os
from catboost import CatBoostClassifier, Pool, CatBoost
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from typing import List
from fastapi import FastAPI, Depends, HTTPException 
from datetime import datetime
from pydantic import BaseModel
from typing import List

'''
ФУНКЦИИ ПО ЗАГРУЗКЕ МОДЕЛЕЙ
'''
# Проверка если код выполняется в лмс, или локально
def get_model_path(path: str) -> str:
    """Просьба не менять этот код"""
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
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



def caching_predictions(feed_data):
    # Get a list of unique users
    unique_users = feed_data['user_id'].unique()

    # Create a group ID based on the 'user_id'
    group_id_dict = {user_id: idx for idx, user_id in enumerate(unique_users)}
    feed_data['group_id'] = feed_data['user_id'].map(group_id_dict)

    # Sort by 'group_id'
    feed_data.sort_values(by='group_id')

    # Create Pool object with the 'group_id' column
    data_pool = Pool(feed_data.drop(columns=['user_id']), group_id=feed_data['group_id'])

    y_pred_proba = model.predict_proba(data_pool)[:, 1]
    # Add the prediction probabilities to the test dataset
    features['pred_proba'] = y_pred_proba

    # Group by 'user_id' and find the top 5 predicted 'post_id' for each user
    top_5_posts = features.groupby('user_id').apply(lambda x: x.nlargest(5, 'pred_proba')['post_id'])
    
    # Convert the multi-index Series to a dictionary
    top_5_posts_dict = top_5_posts.reset_index().groupby('user_id')['post_id'].apply(list).to_dict()
    
    # return the top 5 posts in a dictionary
    return top_5_posts_dict
# Возвращаем список объектов PostGet, содержащий текст и тему выбранных статей





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

top_5_posts_dict = caching_predictions(features)
print("Предсказания сделаны")




'''
FAST API код
'''

# Определяем приложение FastAPI
app = FastAPI()

@app.get("/post/recommendations/", response_model=PostList)
def recommended_posts(id: int, time: datetime, limit: int = 5):
    # Retrieve the post ids
    post_ids = top_5_posts_dict.get(id)

    if not post_ids:
        raise HTTPException(status_code=404, detail="User not found")

    # Retrieve the posts from the database
    query = "SELECT * FROM post_text_df WHERE post_id IN ({})".format(", ".join(map(str, post_ids[:limit])))
    df = pd.read_sql(query, engine)

    # Convert the DataFrame to a list of dictionaries
    posts = df.to_dict('records')

    return {"posts": posts}