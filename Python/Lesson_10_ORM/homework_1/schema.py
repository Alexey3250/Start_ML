import datetime
from typing import Optional
from pydantic import BaseModel # импортировали нужные библиотеки

class PostGet(BaseModel):
    id: int
    text: str
    topic: str
    
    # orm_mode = True is a variable or flag that is often used in the application's 
    # configuration to indicate that the application is running in 
    # Object-Relational Mapping (ORM) mode.
    class Config:
        orm_mode = True
    
class UserGet(BaseModel):
    id: int
    gender: int
    age: int
    country: str
    city: str
    exp_group: int
    os: str
    source: str
    
    class Config:
        orm_mode = True
    
class FeedGet(BaseModel):
    user_id: int
    post_id: int
    action: str
    time: datetime.datetime
    
    # pydantic allows you to declare fields that are not part of the data model
    user: Optional[UserGet]
    post: Optional[PostGet]
    
    class Config:
        orm_mode = True 
