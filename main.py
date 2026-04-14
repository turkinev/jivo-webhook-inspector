import json
import logging
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="JivoChat Webhook Inspector")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)
logger = logging.getLogger(__name__)


def save_payload(event_type: str, payload: dict):
    """Сохраняет каждый хук в отдельный файл для изучения."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = LOG_DIR / f"{timestamp}_{event_type}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return filename


@app.post("/jivo/webhook")
async def jivo_webhook(request: Request):
    """Принимает все события от JivoChat и логирует их."""
    try:
        body = await request.body()
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = payload.get("event_name") or payload.get("event", "unknown")
    filename = save_payload(event_type, payload)

    logger.info(f"[JIVO] event={event_type} | keys={list(payload.keys())} | saved={filename.name}")

    # JivoChat ждет 200 OK — иначе будет ретраить
    return JSONResponse({"result": "ok"})


@app.get("/jivo/logs")
async def list_logs():
    """Показывает список сохраненных хуков с полными данными."""
    files = sorted(LOG_DIR.glob("*.json"), reverse=True)[:50]
    result = []
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            result.append({
                "file": f.name,
                "event": data.get("event_name") or data.get("event", "?"),
                "timestamp": f.stem.split("_")[0] + "_" + f.stem.split("_")[1],
                "payload": data,
            })
        except Exception:
            pass
    return result


@app.get("/jivo/logs/{filename}")
async def get_log(filename: str):
    """Возвращает конкретный хук по имени файла."""
    if "/" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = LOG_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/health")
async def health():
    return {"status": "ok", "logs_count": len(list(LOG_DIR.glob("*.json")))}
