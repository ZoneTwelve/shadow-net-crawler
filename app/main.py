# ./app/main.py
import os
import uvicorn
from fastapi import FastAPI

from app.api.router import router

app = FastAPI(title="Shadow Net Crawler API", version="1.0")
app.include_router(router, prefix="")

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )

