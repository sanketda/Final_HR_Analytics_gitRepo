import os
import json
import atexit
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from tasks import run_diagnosis_task
from pyngrok import ngrok, conf
from dotenv import load_dotenv

# Load env variables
load_dotenv()

NGROK_DOMAIN = os.getenv("NGROK_DOMAIN")
NGROK_AUTHTOKEN = os.getenv("NGROK_AUTHTOKEN")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", 5000))

# Set ngrok authtoken via conf
conf.get_default().auth_token = NGROK_AUTHTOKEN

def start_ngrok(port: int, domain: str):
    # Clean up old tunnels (avoids duplicates on reload)
    for t in ngrok.get_tunnels():
        try:
            ngrok.disconnect(t.public_url)
        except Exception:
            pass

    
    tunnel = ngrok.connect(
        addr=f"http://127.0.0.1:{port}",
        bind_tls=True,
        domain=domain
    )
    print(f" * ngrok tunnel: {tunnel.public_url} -> http://127.0.0.1:{port}")
    atexit.register(ngrok.kill)

if NGROK_DOMAIN:
    start_ngrok(WEBHOOK_PORT, NGROK_DOMAIN)
else:
    ngrok.connect(WEBHOOK_PORT)



app = FastAPI()

@app.get("/hello")
async def hello():
    return {"message": "Webhook is running, listening for job notifs"}

@app.post("/notify", status_code=202)
async def notify(request: Request):
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    job = run_diagnosis_task.delay(data)
    return JSONResponse(content={"task_id": job.id}, status_code=202)
