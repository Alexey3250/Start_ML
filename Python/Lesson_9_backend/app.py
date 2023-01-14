from fastapi import FastAPI, HTTPException
from datetime import timedelta, date
from pydantic import BaseModel
import requests

app = FastAPI(debug=True)

class User(BaseModel):
    name: str
    surname: str
    age: int
    registration_date: date
    
    class Config:
        orm_mode = True
    
@app.post("/user/validate")
def validate_user(user: User):
    # если хотя бы одно из полей пустое, то возвращать код 422
    if not isinstance(user.name, str) or not isinstance(user.surname, str) or not isinstance(user.age, int) or not isinstance(user.registration_date, date):
        raise HTTPException(422)

    else: 
        return ("Will add user: {} {} with age {}".format(user.name, user.surname, user.age))
