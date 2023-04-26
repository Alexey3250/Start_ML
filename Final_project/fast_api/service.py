import os
from typing import List
from fastapi import FastAPI
from schema import PostGet
from datetime import datetime

app = FastAPI()

@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int, 
		time: datetime, 
		limit: int = 10) -> List[PostGet]:
     pass