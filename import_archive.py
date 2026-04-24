"""
Импорт пропущенных chat_finished из archive.txt в ClickHouse.

Запуск:
    python3 import_archive.py [--dry-run] [--file PATH]

--dry-run   только показать что будет вставлено, без записи в БД
--file      путь к файлу архива (по умолчанию archive.txt рядом со скриптом)
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


# ── .env ──────────────────────────────────────────────────────────────────────

def load_dotenv(path: str = "/opt/jivo_inspector/.env"):
    env_path = Path(path)
    if not env_path.exists():
        # fallback: .env рядом со скриптом
        env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from ai_processor import analyze_and_save

CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")


# ── ClickHouse ─────────────────────────────────────────────────────────────────

def ch_request(query: str, data: bytes = None, timeout: int = 30) -> str:
    params = urllib.parse.urlencode({
        "query":    query,
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    resp = urllib.request.urlopen(req, timeout=timeout)
    return resp.read().decode()


def get_existing_chat_ids() -> set:
    raw = ch_request(
        "SELECT DISTINCT chat_id FROM raw_dialogs "
        "WHERE event_name = 'chat_finished' FORMAT JSONEachRow"
    )
    ids = set()
    for line in raw.strip().splitlines():
        try:
            ids.add(int(json.loads(line)["chat_id"]))
        except Exception:
            pass
    return ids


def extract_dialog_row(payload: dict) -> dict:
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


def insert_payload(payload: dict):
    chat_id    = int(payload.get("chat_id", 0))
    event_name = payload.get("event_name", "chat_finished")
    source     = payload.get("source", "jivo")

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

    dialog_row = json.dumps(extract_dialog_row(payload), ensure_ascii=False)
    ch_request(
        "INSERT INTO dialogs FORMAT JSONEachRow",
        data=dialog_row.encode("utf-8"),
    )


# ── Парсер архива ──────────────────────────────────────────────────────────────

REQUEST_RE = re.compile(
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} POST http[^\n]+"
)


def parse_archive(path: Path) -> list[dict]:
    """Возвращает список payload-словарей из архива."""
    text = path.read_text(encoding="utf-8", errors="replace")

    # Разбиваем на блоки по строке с timestamp
    parts = REQUEST_RE.split(text)
    results = []

    for part in parts:
        # Вырезаем часть до Response code (это тело запроса)
        request_body = part.split("Response code:")[0].strip()
        if not request_body:
            continue
        try:
            payload = json.loads(request_body)
            if isinstance(payload, dict):
                results.append(payload)
        except json.JSONDecodeError:
            pass

    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="не писать в БД, только показать что будет вставлено")
    parser.add_argument("--file", default=str(Path(__file__).parent / "archive.txt"),
                        help="путь к файлу архива")
    args = parser.parse_args()

    archive_path = Path(args.file)
    if not archive_path.exists():
        print(f"[error] Файл не найден: {archive_path}")
        return

    print(f"Читаем {archive_path} ...")
    all_payloads = parse_archive(archive_path)
    print(f"Всего записей в архиве: {len(all_payloads)}")

    finished = [p for p in all_payloads if p.get("event_name") == "chat_finished"]
    print(f"Из них chat_finished: {len(finished)}")

    if not finished:
        print("Нечего импортировать.")
        return

    print("Получаем существующие chat_id из БД ...")
    existing = get_existing_chat_ids()
    print(f"Уже в БД: {len(existing)}")

    to_insert = [p for p in finished if int(p.get("chat_id", 0)) not in existing]
    print(f"К вставке: {len(to_insert)}\n")

    if not to_insert:
        print("Все диалоги уже есть в БД.")
        return

    if args.dry_run:
        print("=== DRY RUN ===")
        for p in to_insert:
            visitor = (p.get("visitor") or {}).get("name", "?")
            print(f"  chat_id={p.get('chat_id')} | visitor={visitor}")
        return

    ok = fail = 0
    for i, payload in enumerate(to_insert, 1):
        payload["source"] = "jivo"
        chat_id = payload.get("chat_id")
        try:
            insert_payload(payload)
            print(f"[{i}/{len(to_insert)}] CH OK chat_id={chat_id}")
        except Exception as e:
            print(f"[{i}/{len(to_insert)}] CH FAIL chat_id={chat_id}: {e}")
            fail += 1
            continue

        try:
            analyze_and_save(payload)
            print(f"[{i}/{len(to_insert)}] AI OK chat_id={chat_id}")
            ok += 1
        except Exception as e:
            print(f"[{i}/{len(to_insert)}] AI FAIL chat_id={chat_id}: {e}")
            fail += 1

        if i < len(to_insert):
            time.sleep(0.5)

    print(f"\n=== Готово: {ok} вставлено с AI, {fail} ошибок ===")


if __name__ == "__main__":
    main()
