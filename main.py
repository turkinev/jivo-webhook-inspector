import asyncio
import json
import logging
import os
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, Response

app = FastAPI(title="JivoChat Webhook Inspector")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)
logger = logging.getLogger(__name__)

# Пул потоков для синхронных CH-вызовов (не блокируют event loop)
executor = ThreadPoolExecutor(max_workers=4)

# ClickHouse — настройки через переменные окружения
CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")


def ch_request(query: str, data: bytes = None, timeout: int = 10) -> str:
    """Выполняет запрос к ClickHouse через HTTP API."""
    params = urllib.parse.urlencode({
        "query": query,
        "user": CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    resp = urllib.request.urlopen(req, timeout=timeout)
    return resp.read().decode()


def save_payload(event_type: str, payload: dict) -> Path:
    """Сохраняет каждый хук в отдельный файл для изучения."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = LOG_DIR / f"{timestamp}_{event_type}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return filename


def extract_dialog_row(payload: dict) -> dict:
    """Разбирает payload chat_finished в плоскую структуру для jivo_chat_dialogs."""
    visitor  = payload.get("visitor") or {}
    agents   = payload.get("agents") or []
    agent    = agents[0] if agents else {}
    page     = payload.get("page") or {}
    session  = payload.get("session") or {}
    geoip    = session.get("geoip") or {}
    chat     = payload.get("chat") or {}
    messages = chat.get("messages") or []

    visitor_texts = [m["message"] for m in messages if m.get("type") == "visitor"]
    agent_texts   = [m["message"] for m in messages if m.get("type") == "agent"]

    return {
        "event_name":           payload.get("event_name", "chat_finished"),
        "event_timestamp":      payload.get("event_timestamp"),          # Unix int → DateTime
        "chat_id":              int(payload.get("chat_id", 0)),
        "widget_id":            payload.get("widget_id") or "",
        "visitor_id":           int(visitor.get("number", 0)),
        "visitor_name":         visitor.get("name"),
        "visitor_chats_count":  int(visitor.get("chats_count") or 0),
        "operator_id":          int(agent["id"]) if agent.get("id") else None,
        "operator_name":        agent.get("name"),
        "page_url":             page.get("url"),
        "page_title":           page.get("title"),
        "geo_country":          geoip.get("country"),
        "geo_region":           geoip.get("region"),
        "geo_city":             geoip.get("city"),
        "chat_messages_json":   json.dumps(messages, ensure_ascii=False),
        "invite_timestamp":     chat.get("invite_timestamp"),            # Unix int → DateTime
        "chat_rate":            payload.get("chat_rate"),
        "plain_messages":       payload.get("plain_messages") or "",
        "full_dialog_text":     payload.get("plain_messages") or "",
        "visitor_messages_text": "\n".join(visitor_texts),
        "agent_messages_text":   "\n".join(agent_texts),
    }


def _insert_sync(payload: dict):
    """Синхронная вставка в CH — запускается в thread pool."""
    chat_id    = int(payload.get("chat_id", 0))
    event_name = payload.get("event_name", "chat_finished")

    # 1. raw_jivo_chat — полный payload как JSON-строка
    raw_row = json.dumps({
        "event_name":   event_name,
        "chat_id":      chat_id,
        "payload_json": json.dumps(payload, ensure_ascii=False),
    }, ensure_ascii=False)
    ch_request(
        "INSERT INTO raw_jivo_chat (event_name, chat_id, payload_json) FORMAT JSONEachRow",
        data=raw_row.encode("utf-8"),
    )

    # 2. jivo_chat_dialogs — структурированные поля
    dialog_row = json.dumps(extract_dialog_row(payload), ensure_ascii=False)
    ch_request(
        "INSERT INTO jivo_chat_dialogs FORMAT JSONEachRow",
        data=dialog_row.encode("utf-8"),
    )


def _health_check_sync():
    """Синхронная проверка CH — запускается в thread pool."""
    result = ch_request("SELECT 1")
    assert result.strip() == "1", f"Unexpected response: {result}"


async def insert_to_clickhouse(payload: dict):
    """Асинхронная обёртка над вставкой в CH."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, _insert_sync, payload)


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

    logger.info(f"[JIVO] event={event_type} | chat_id={payload.get('chat_id')} | saved={filename.name}")

    # Сохраняем в ClickHouse только завершенные чаты
    if event_type == "chat_finished":
        try:
            await insert_to_clickhouse(payload)
            logger.info(f"[CH] inserted chat_id={payload.get('chat_id')}")
        except Exception as e:
            logger.error(f"[CH] insert failed: {e}")
            # Не возвращаем ошибку JivoChat — хук уже сохранен в файл

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
    return Response(
        content=json.dumps(result, ensure_ascii=False, indent=2),
        media_type="application/json",
    )


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
    """Статус сервиса + проверка подключения к ClickHouse."""
    ch_status = "ok"
    try:
        loop = asyncio.get_event_loop()
        await asyncio.wait_for(
            loop.run_in_executor(executor, _health_check_sync),
            timeout=6.0,
        )
    except asyncio.TimeoutError:
        ch_status = "error: timeout"
    except Exception as e:
        ch_status = f"error: {e}"

    return {
        "status": "ok",
        "logs_count": len(list(LOG_DIR.glob("*.json"))),
        "clickhouse": ch_status,
    }
