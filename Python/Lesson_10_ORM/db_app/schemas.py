import datetime
from typing import Optional

from pydantic import BaseModel, Field


class UserRegister(BaseModel):
    name: str
    surname: str


class UserGet(BaseModel):
    first_name: str = ""
    surname: str = ""
    recommended_by: Optional["UserGet"] = None

    class Config:
        orm_mode = True


class BookingGet(BaseModel):
    member_id: int
    member: UserGet
    facility_id: int
    start_time: datetime.datetime
    slots: int

    class Config:
        orm_mode = True
