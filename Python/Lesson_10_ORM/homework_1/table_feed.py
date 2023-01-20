from database import Base
from table_post import Post
from table_user import User
from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, PrimaryKeyConstraint
from sqlalchemy.orm import relationship

class Feed(Base):
    __tablename__ = "feed_action"
    # used_id is a foreign key to the user table
    user_id = Column(Integer, ForeignKey("user.id"), primary_key=True, index=True)
    # post_id is a foreign key to the post table
    post_id = Column(Integer, ForeignKey("post.id"), primary_key=True, index=True)
    action = Column(String)
    time = Column(TIMESTAMP)
    
    # create a relationship to the user table
    user = relationship(User)
    post = relationship(Post)
    