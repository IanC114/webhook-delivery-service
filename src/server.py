import logging
from collections import defaultdict
from json import JSONDecodeError

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response, Request
import uvicorn

import prometheus_client


class ConnectionManager:
    def __init__(self):
        self.websockets = defaultdict(list)

    async def connect(self, hook_id, websocket: WebSocket):
        await websocket.accept()
        self.websockets[hook_id].append(websocket)
        clients_count.inc()

    def disconnect(self, hook_id, websocket: WebSocket):
        self.websockets[hook_id].remove(websocket)
        clients_count.dec()

        if not self.websockets[hook_id]:
            del self.websockets[hook_id]

    async def broadcast(self, hook_id, data):
        for websocket in self.websockets.get(hook_id, []):
            await websocket.send_json(data)


app = FastAPI()
manager = ConnectionManager()
clients_count = prometheus_client.Gauge(
    "clients_count",
    "Number of clients"
)


@app.post("/webhook/")
@app.post("/webhook/{hook_id}")
async def post_webhook(request: Request, response: Response, hook_id: str | None = None):
    try:
        if hook_id:
            datajson = await request.json()
            response.status_code = 200
            await manager.broadcast(hook_id, datajson)
        else:
            response.status_code = 404
    except (JSONDecodeError, TypeError):
        logging.exception("Invalid JSON!")
        response.status_code = 400
    finally:
        return {"status_code": response.status_code}


@app.websocket("/tunnel/{hook_id}")
async def websocket_endpoint(hook_id: str, websocket: WebSocket):
    await manager.connect(hook_id, websocket)

    # keep connection alive to detect disconnections
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(hook_id, websocket)


@app.get('/metrics')
def get_metrics():
    return Response(
        content=prometheus_client.generate_latest(),
        media_type="text/plain"
    )


if __name__ == "__main__":
    uvicorn.run("src.server:app", host="localhost", port=5000, reload=True)
