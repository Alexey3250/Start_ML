# Импортируем необходимые модули и библиотеки
import os
from catboost import CatBoostClassifier
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from typing import List
from fastapi import FastAPI, Depends
from schema import PostGet
from datetime import datetime

# Определяем функцию для получения пути к модели
def get_model_path(path: str) -> str:
    # Проверяем, выполняется ли код в LMS, и устанавливаем путь для загрузки модели
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

# Определяем функцию для загрузки модели машинного обучения
def load_models():
    model_path = get_model_path("/my/super/path")
    # Загружаем модель из файла по пути model_path
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path)
    return model

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

# Определяем функцию для получения сессии базы данных
def get_db():
    with SessionLocal() as db:
        return db

# Определяем приложение FastAPI
app = FastAPI()

# Определяем функцию для получения списка рекомендуемых статей
@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int,
		time: datetime,
		limit: int = 5,
		db: Session = Depends(get_db)) -> List[PostGet]:
	# Получаем признаки пользователя из таблицы признаков features
	user_features = features[features['user_id']==id]
	# Предсказываем вероятности понравившихся статей с помощью загруженной модели
	user_features['proba'] = model.predict_proba(user_features)[:, 1]
	# Выбираем limit статей с наибольшими вероятностями
	preds_id = user_features.sort_values(by='proba', ascending=False)['post_id'][:limit].values.tolist()
	preds = []
	# Для каждой выбранной статьи выполняем запрос к базе данных, чтобы получить ее текст и тему
	for i in preds_id:
		preds.append(db.query(Post).filter(Post.id == i).one_or_none())
	# Возвращаем список объектов PostGet, содержащий текст и тему выбранных статей
	return preds