from fastapi import FastAPI, HTTPException
from datetime import timedelta, date
from pydantic import BaseModel

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
    if not user.name or not user.surname or not user.age or not user.registration_date:
        raise HTTPException(422)
    else: 
        return ("Will add user: {} {} with age {}".format(user.name, user.surname, user.age))