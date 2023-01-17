from database import Base, engine, SessionLocal
from sqlalchemy import Column, Integer, String, func

class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True, index=True)
    gender = Column(String) #
    age = Column(Integer)
    country = Column(String)
    city = Column(String)
    exp_group = Column(Integer)
    os = Column(String)
    source = Column(String) #

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    
    # create a new session
    session = SessionLocal()
    # perform the query
    query = (session
        .query(User.country, User.os, func.count(User.id))
        .filter(User.exp_group == 3)
        .group_by(User.country, User.os)
        .having(func.count(User.id) > 100)
        .order_by(func.count(User.id).desc())
    )
    result = query.all()
    print(result)
    
    # close the session
    session.close()
    
    