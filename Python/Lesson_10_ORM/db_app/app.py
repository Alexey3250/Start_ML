from typing import List

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import Booking, Facility, Member
from .schemas import BookingGet, UserGet

app = FastAPI()


def get_db():
    with SessionLocal() as db:
        return db


@app.get("/user/all", response_model=List[UserGet])
def get_all_users(limit: int = 10, db: Session = Depends(get_db)):
    return db.query(Member).limit(limit).all()


@app.get("/facility/all")
def get_all_facilities(limit: int = 10, db: Session = Depends(get_db)):
    return db.query(Facility).limit(limit).all()


@app.get("/booking/all", response_model=List[BookingGet])
def get_all_bookings(limit: int = 10, db: Session = Depends(get_db)):
    return db.query(Booking).limit(limit).all()
