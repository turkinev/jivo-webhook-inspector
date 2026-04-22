import asyncio
import json
import logging
import os
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, Response

from ai_processor import analyze_and_save
from log_routes import router as log_router, ensure_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dialog Analytics Webhook")
app.include_router(log_router)


@app.on_event("startup")
def on_startup():
    try:
        ensure_table()
        logger.info("[startup] support_log_edits OK")
    except Exception as e:
        logger.warning(f"[startup] support_log_edits: {e}")


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


def extract_dialog_row(payload: dict) -> dict:
    """Разбирает payload chat_finished в плоскую структуру для dialogs."""
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
        "source":                payload.get("source", "jivo"),
        "event_name":            payload.get("event_name", "chat_finished"),
        "event_timestamp":       payload.get("event_timestamp"),
        "chat_id":               int(payload.get("chat_id", 0)),
        "widget_id":             payload.get("widget_id") or "",
        "visitor_id":            int(visitor.get("number", 0)),
        "visitor_name":          visitor.get("name"),
        "visitor_chats_count":   int(visitor.get("chats_count") or 0),
        "operator_id":           int(agent["id"]) if agent.get("id") else None,
        "operator_name":         agent.get("name"),
        "page_url":              page.get("url"),
        "page_title":            page.get("title"),
        "geo_country":           geoip.get("country"),
        "geo_region":            geoip.get("region"),
        "geo_city":              geoip.get("city"),
        "chat_messages_json":    json.dumps(messages, ensure_ascii=False),
        "invite_timestamp":      chat.get("invite_timestamp"),
        "chat_rate":             payload.get("chat_rate"),
        "plain_messages":        payload.get("plain_messages") or "",
        "full_dialog_text":      payload.get("plain_messages") or "",
        "visitor_messages_text": "\n".join(visitor_texts),
        "agent_messages_text":   "\n".join(agent_texts),
    }


def _insert_sync(payload: dict):
    """Синхронная вставка в CH — запускается в thread pool."""
    chat_id    = int(payload.get("chat_id", 0))
    event_name = payload.get("event_name", "chat_finished")
    source     = payload.get("source", "jivo")

    # 1. raw_dialogs — полный payload как JSON-строка
    raw_row = json.dumps({
        "source":       source,
        "event_name":   event_name,
        "chat_id":      chat_id,
        "payload_json": json.dumps(payload, ensure_ascii=False),
    }, ensure_ascii=False)
    ch_request(
        "INSERT INTO raw_dialogs (source, event_name, chat_id, payload_json) FORMAT JSONEachRow",
        data=raw_row.encode("utf-8"),
    )

    # 2. dialogs — структурированные поля
    dialog_row = json.dumps(extract_dialog_row(payload), ensure_ascii=False)
    ch_request(
        "INSERT INTO dialogs FORMAT JSONEachRow",
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
async def jivo_webhook(request: Request, background_tasks: BackgroundTasks):
    """Принимает все события от JivoChat."""
    try:
        body = await request.body()
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = payload.get("event_name") or payload.get("event", "unknown")
    payload["source"] = "jivo"

    logger.info(f"[JIVO] event={event_type} | chat_id={payload.get('chat_id')}")

    if event_type == "chat_finished":
        try:
            await insert_to_clickhouse(payload)
            logger.info(f"[CH] inserted chat_id={payload.get('chat_id')}")
        except Exception as e:
            logger.error(f"[CH] insert failed: {e}")

        background_tasks.add_task(analyze_and_save, payload)

    return JSONResponse({"result": "ok"})


@app.get("/jivo/logs")
async def list_logs():
    """Последние 50 диалогов из raw_dialogs."""
    def _query():
        return ch_request(
            "SELECT source, chat_id, event_name, received_at, payload_json "
            "FROM raw_dialogs "
            "ORDER BY received_at DESC "
            "LIMIT 50 "
            "FORMAT JSONEachRow"
        )

    loop = asyncio.get_event_loop()
    try:
        raw = await loop.run_in_executor(executor, _query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = []
    for line in raw.strip().splitlines():
        try:
            row = json.loads(line)
            row["payload"] = json.loads(row.pop("payload_json", "{}"))
            rows.append(row)
        except Exception:
            pass

    return Response(
        content=json.dumps(rows, ensure_ascii=False, indent=2),
        media_type="application/json",
    )


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
        "clickhouse": ch_status,
    }
