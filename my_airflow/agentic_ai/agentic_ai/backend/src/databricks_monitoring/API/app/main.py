from fastapi import FastAPI
import uvicorn
from app.api import api

app = FastAPI()

# Register routes
app.include_router(api.router, prefix="/v1", tags=["API"])


if __name__ == "__main__": 
    uvicorn.run(app, host="0.0.0.0", port=8000)