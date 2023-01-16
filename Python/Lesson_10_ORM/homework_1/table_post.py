from database import Base, engine, SessionLocal
from sqlalchemy import Column, Integer, String

class Post(Base):
    __tablename__ = "post"
    id = Column(Integer, primary_key=True, index=True)
    text = Column(String)
    topic = Column(String)


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    
    # create a new session
    session = SessionLocal()
    # perform the query
    business_posts_ids = session.query(Post.id).filter(Post.topic == "business").order_by(Post.id.desc()).limit(10).all()
    print([id[0] for id in business_posts_ids])
    # close the session
    session.close()
    