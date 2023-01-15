from database import Base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
import datetime

class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    body = Column(String)
    published = Column(DateTime, default=datetime.datetime.utcnow)
    user_id = Column(Integer, ForeignKey("users.id"))