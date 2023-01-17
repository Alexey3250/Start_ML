from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

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