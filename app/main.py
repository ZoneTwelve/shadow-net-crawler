# ./app/main.py
import os
import uvicorn
from fastapi import FastAPI

# Absolute import – works both as a script and as a module.
from app.api.router import router

app = FastAPI(title="Recursive Crawler Queue API")
app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )