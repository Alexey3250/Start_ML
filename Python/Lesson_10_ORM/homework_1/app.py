from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import func

from typing import List, Optional

from database import SessionLocal, Base
from table_feed import Feed
from table_post import Post
from table_user import User
from schema import FeedGet, PostGet, UserGet


app = FastAPI()

@app.get("/user/{id}", response_model=UserGet)
def get_user(id: int):
    session = SessionLocal()
    user = session.query(User).filter(User.id == id).one_or_none()
    if not user:
        raise HTTPException(404, "user not found!")
    else:
        return user

@app.get("/post/{id}", response_model=PostGet)
def get_post(id: int):
    session = SessionLocal()
    post = session.query(Post).filter(Post.id == id).one_or_none()
    if not post:
        raise HTTPException(404, "post not found!")
    else:
        return post

@app.get("/user/{id}/feed", response_model=List[FeedGet])
def get_user_feed(id: int, limit: int = 10):
    session = SessionLocal()
    
    # вернуть все действия из feed для пользователя с id = {id} (из запроса).
    feeds = session.query(Feed) \
        .filter(Feed.user_id == id) \
        .order_by(Feed.time.desc()) \
        .limit(limit) \
        .all()
    
    # проверить если список возвращается пустым, то вернуть ошибку 200, но с пустым списком
    if not feeds:
        raise HTTPException(200, "feed not found!")
    
    return feeds

@app.get("/post/{id}/feed", response_model=List[FeedGet])
def get_post_feed(id: int, limit: int = 10):
    session = SessionLocal()
    
    # вернуть все действия из feed для поста с id = {id} (из запроса).
    feeds = session.query(Feed) \
        .filter(Feed.post_id == id) \
        .order_by(Feed.time.desc()) \
        .limit(limit) \
        .all()
        
    # проверить если список возвращается пустым, то вернуть ошибку 200, но с пустым списком
    if not feeds:
        raise HTTPException(200, "feed not found!")
    
    return feeds

@app.get("/post/reccomendations/")
def get_recommended_feed(id: int, limit: int = 10):
    session = SessionLocal()
    '''
    Возвращает 10 постов, которые имеют наибольшее количество лайков

    1. Запрос к сессии по таблице Post
    2. Фильтр по лайкам
    3. Джойн таблицы Post
    4. Группировка по Post.id
    5. Сортировка по убыванию func.count(Post.id)
    6. Лимит строк и .all()
    '''
   
    
    recommended_feed = session.query(Post) \
        .join(Feed, Feed.post_id == Post.id) \
        .filter(Feed.action == 'like') \
        .group_by(Post.id) \
        .order_by(func.count(Post.id).desc()) \
        .limit(limit) \
        .all()
    
    if id is None:
        raise HTTPException(400, "id parameter is missing")

    
    return recommended_feed