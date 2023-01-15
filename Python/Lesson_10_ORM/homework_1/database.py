from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# this is a connection string to a database
SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"

# engine is a connection to a database
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# create a session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# create a base class for models
Base = declarative_base()
