# Импортируем необходимые модули и библиотеки
import os
from catboost import CatBoostClassifier, Pool, CatBoost
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from typing import List
from fastapi import FastAPI, Depends 
from datetime import datetime
from pydantic import BaseModel

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
    query = "a-efimik_features_lesson_22_500MB"
    return batch_load_sql(query)

model = load_models()
features = load_features()

# Определяем переменные для подключения к базе данных
SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Определяем класс Post для работы с таблицей базы данных post
class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)

class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True

# Определяем функцию для получения сессии базы данных
def get_db():
    with SessionLocal() as db:
        return db



def prepare_data_for_prediction(user_id: int, features: pd.DataFrame) -> Pool:
    # Получение фичей по пользователю
    user_features = features[features['user_id'] == user_id]

    # Подготовка данных для предсказаний
    X = user_features.drop(['user_id', 'post_id_x'], axis=1)

    # Получение уникальных user_id
    unique_user_ids = X['user_id'].unique()
    # Создание словаря с соответствием user_id и group_id
    group_id_dict = {user_id: idx for idx, user_id in enumerate(unique_user_ids)}
    # Добавление group_id в датафрейм
    X['group_id'] = X['user_id'].map(group_id_dict)
    # Сортировка по group_id
    X = X.sort_values(by='group_id')

    # Создание Pool
    data_pool = Pool(X.drop(columns=['user_id']), group_id=X['group_id'])

    return data_pool

# Возвращаем список объектов PostGet, содержащий текст и тему выбранных статей

'''
FAST API код
'''

# Определяем приложение FastAPI
app = FastAPI()

@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
        id: int,
        time: datetime,
        limit: int = 5,
        db: Session = Depends(get_db)) -> List[PostGet]:
    
    # подготовить данные для предсказания
    data_pool = prepare_data_for_prediction(id, features)
    
    # делаем предсказания с использованием обученной модели и data_pool
    predictions = model.predict_proba(data_pool)[:, 1]

    # Добавляем предсказания в dataframe user_features
    user_features = features[features['group_id'] == id]
    user_features['proba'] = predictions

    # Выбираем limit постов с наибольшими вероятностями
    preds_id = user_features.sort_values(by='proba', ascending=False)['post_id_x'][:limit].values.tolist()
    preds = []

    # Для каждого выбранного поста выполняем запрос к базе данных, чтобы получить его текст и тему
    for i in preds_id:
        post = db.query(Post).filter(Post.id == i).one_or_none()
        preds.append(PostGet(id=post.id, text=post.text, topic=post.topic))
        
    # Возвращаем список объектов PostGet, содержащий текст и тему выбранных статей
    return preds