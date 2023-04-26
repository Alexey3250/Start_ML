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

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():
    model_path = get_model_path("/my/super/path")
    # LOAD MODEL HERE PLS :)
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path)
    return model

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
    return batch_load_sql('SELECT * FROM public.yancharskaya_features_lesson_22')

model = load_models()
features = load_features()

SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db
   
class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)

@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int, 
		time: datetime, 
		limit: int = 5,
        db: Session = Depends(get_db)) -> List[PostGet]:
    user_features = features[features['user_id']==id]
    user_features['proba'] = model.predict_proba(user_features)[:, 1]
    preds_id = user_features.sort_values(by='proba', ascending=False)['post_id'][:limit].values.tolist()
    preds = []
    for i in preds_id:
        preds.append(db.query(Post).filter(Post.id == i).one_or_none())
    return preds