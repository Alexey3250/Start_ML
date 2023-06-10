import os
from catboost import CatBoostClassifier, Pool, CatBoost
import pandas as pd
from sqlalchemy import create_engine
from fastapi import FastAPI, Depends, HTTPException, Query 
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List
import logging
import pydantic
from tqdm import tqdm
import time
from datetime import datetime, time, date



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
    model_path = get_model_path("catboost_model.cbm")
    model = CatBoostWrapper()
    model.load_model(model_path)
    return model

'''
Получение данных из базы данных
'''
# Определяем функцию для получения данных из базы данных PostgreSQL
def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    total_rows = 7689263  # total number of rows in your dataset

    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)

    chunks = []
    with tqdm(total=total_rows, desc="Loading data") as pbar:
        for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
            chunks.append(chunk_dataframe)
            pbar.update(CHUNKSIZE)
            
    conn.close()

    return pd.concat(chunks, ignore_index=True)

def load_features() -> pd.DataFrame:
    query = "ilia_svetlichnyi_features_lesson_22_v3"
    return batch_load_sql(query)




def predict_posts(user_id: int, limit: int):

    # Фильтруем записи, относящиеся к конкретному user_id
    user_features = features[features.user_id == user_id]
    
    # Вычисляем вероятности для каждого post_id для конкретного user_id
    user_features['probas'] = model.predict_proba(user_features.drop('user_id', axis=1))[:, 1]
    
    # Сортируем DataFrame по 'probas' в порядке убывания и получаем первые 'limit' записей
    top_posts = user_features.sort_values('probas', ascending=False).iloc[:limit]
    
    # Возвращаем 'post_id' лучших записей в виде списка
    return top_posts['post_id'].tolist()



def load_post_texts_df():
    global post_texts_df
    print("Загружаю все тексты постов...")
    query = "SELECT * FROM post_text_df"
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    post_texts_df = pd.read_sql(query, con=engine)
    print("Все тексты постов успешно загружены в память.")

def load_post_texts(post_ids: List[int]) -> List[dict]:
    global post_texts_df
    if post_texts_df is None:
        raise ValueError("Таблица с текстами постов не загружена. Сначала вызовите функцию load_post_texts_df().")
        
    # Извлекаем записи из памяти
    records_df = post_texts_df[post_texts_df['post_id'].isin(post_ids)]
    return records_df.to_dict("records")


'''
ЗАГРУЗКА МОДЕЛЕЙ И ФИЧЕЙ (БЕЗ ПОТОКОВ)

model = load_models()
print("Модель загружена")
features = load_features_from_csv("C:/Users/Alex/Desktop/data_sample_10.csv") #load_features()
print("Данные загружены")
# Глобальная переменная для хранения данных
load_post_texts_df()
'''    

from concurrent.futures import ThreadPoolExecutor

'''
ЗАГРУЗКА МОДЕЛЕЙ И ФИЧЕЙ В ПОТОКАХ
'''
def load_model_and_print():
    model = load_models()
    print("Модель загружена")
    return model

def load_features_and_print():
    features = pd.read_csv("C:/Users/Alex/Desktop/data_sample_10.csv") #load_features_from_csv("C:/Users/Alex/Desktop/data_sample_10.csv")  # load_features()
    print("Данные загружены")
    return features

def load_posts_df_and_print():
    # Глобальная переменная для хранения данных
    load_post_texts_df()
    print("Таблица с текстами постов загружена")
    return True

executor = ThreadPoolExecutor(max_workers=3)
model_future = executor.submit(load_model_and_print)
features_future = executor.submit(load_features_and_print)
posts_df_future = executor.submit(load_posts_df_and_print)

# Wait until all threads are done
executor.shutdown(wait=True)

model = model_future.result()
print("Модель загружена")
features = features_future.result()
print("Данные загружены")


'''
FASTAPI
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
    post_ids = predict_posts(id, limit)
    records = load_post_texts(post_ids)

    posts = []
    for rec in records:
        rec["id"] = rec.pop("post_id")  # change "post_id" to "id"
        try:
            posts.append(PostGet(**rec))
        except pydantic.error_wrappers.ValidationError as e:
            print(f"Validation error for record {rec}: {e}")
    return posts


