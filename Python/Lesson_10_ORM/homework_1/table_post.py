from database import Base, engine
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
import datetime

class Post(Base):
    __tablename__ = "post"
    id = Column(Integer, primary_key=True, index=True)
    text = Column(String)
    topic = Column(String)

business_posts_ids = session.query(Post.id).finter(Post.topic == "business").order_by(Post.id.desc()).limit(10).all()

print([id[0] for id in business_posts_ids])

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)