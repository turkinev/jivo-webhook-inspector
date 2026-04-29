"""
Поллер DB-источников: забирает завершённые диалоги и отправляет в AI.
Запускается по cron или вручную.

Запуск:
    python3 poller.py [--source site_pm] [--dry-run] [--limit N]

--source    конкретный источник (по умолчанию все)
--dry-run   не запускать AI, только показать что нашли
--limit N   обработать не более N диалогов
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path


def load_dotenv(path: str = "/opt/jivo_inspector/.env"):
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        os.environ[key.strip()] = value


load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))
from ai_processor import analyze_and_save, CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE

# ---------------------------------------------------------------------------
# Cursor: храним последний обработанный timestamp в CH
# ---------------------------------------------------------------------------

CURSOR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS poller_cursor (
    source      String,
    last_seen   DateTime DEFAULT '1970-01-01 00:00:00'
) ENGINE = ReplacingMergeTree
ORDER BY source
"""


def ch_exec(sql: str, data: bytes = None) -> str:
    # SELECT/DDL — SQL в теле; INSERT с данными — SQL в URL query=, данные в теле
    if data is not None:
        params = urllib.parse.urlencode({
            "query": sql,
            "user": CH_USER, "password": CH_PASSWORD, "database": CH_DATABASE,
        })
        body = data
    else:
        params = urllib.parse.urlencode({
            "user": CH_USER, "password": CH_PASSWORD, "database": CH_DATABASE,
        })
        body = sql.encode()
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return resp.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"CH {e.code}: {e.read().decode()[:300]}") from None


def ensure_cursor_table():
    ch_exec(CURSOR_TABLE_SQL)


def get_cursor(source: str) -> datetime:
    """Возвращает timestamp последнего обработанного диалога."""
    try:
        result = ch_exec(
            f"SELECT last_seen FROM poller_cursor FINAL WHERE source = '{source}' FORMAT JSONEachRow"
        ).strip()
        if result:
            ts = json.loads(result)["last_seen"]
            return datetime.fromisoformat(ts)
    except Exception:
        pass
    return datetime(1970, 1, 1)


def set_cursor(source: str, ts: datetime):
    """Сохраняет timestamp последнего обработанного диалога."""
    row = json.dumps({"source": source, "last_seen": ts.isoformat()})
    ch_exec(
        "INSERT INTO poller_cursor (source, last_seen) FORMAT JSONEachRow",
        data=row.encode(),
    )


def get_processed_ids(source: str) -> set:
    """Возвращает set chat_id уже обработанных диалогов из dialog_analysis."""
    try:
        result = ch_exec(
            f"SELECT DISTINCT chat_id FROM dialog_analysis WHERE source = '{source}' FORMAT JSONEachRow"
        )
        return {int(json.loads(line)["chat_id"]) for line in result.strip().splitlines() if line}
    except Exception as e:
        logger.warning(f"[poller] Не удалось загрузить обработанные ID: {e}")
        return set()


def save_raw(dialog) -> None:
    """Сохраняет диалог в raw_dialogs."""
    row = json.dumps({
        "source":       dialog.source,
        "event_name":   "chat_finished",
        "chat_id":      dialog.chat_id,
        "payload_json": dialog.raw_json,
    }, ensure_ascii=False)
    ch_exec(
        "INSERT INTO raw_dialogs (source, event_name, chat_id, payload_json) FORMAT JSONEachRow",
        data=row.encode(),
    )


def save_dialog(dialog) -> None:
    """Сохраняет структурированные поля диалога в dialogs."""
    row = json.dumps({
        "source":                dialog.source,
        "event_name":            "chat_finished",
        "event_timestamp":       dialog.finished_at,
        "chat_id":               dialog.chat_id,
        "widget_id":             getattr(dialog, "widget_id", ""),
        "visitor_id":            int(dialog.visitor_id) if str(dialog.visitor_id).isdigit() else 0,
        "visitor_name":          dialog.visitor_name,
        "visitor_chats_count":   dialog.chats_count,
        "operator_id":           None,
        "operator_name":         dialog.operator_name,
        "page_url":              dialog.page_url,
        "page_title":            "",
        "geo_country":           "",
        "geo_region":            "",
        "geo_city":              "",
        "chat_messages_json":    "[]",
        "invite_timestamp":      None,
        "chat_rate":             None,
        "plain_messages":        dialog.plain_messages,
        "full_dialog_text":      dialog.plain_messages,
        "visitor_messages_text": "",
        "agent_messages_text":   "",
    }, ensure_ascii=False)
    ch_exec(
        "INSERT INTO dialogs FORMAT JSONEachRow",
        data=row.encode(),
    )


# ---------------------------------------------------------------------------
# Реестр источников
# ---------------------------------------------------------------------------

SOURCES = {
    "site_pm": "connectors.site_pm",
    "claim":   "connectors.claim",
}


def run_source(source_name: str, dry_run: bool, limit: int, days_back: int = 1):
    if source_name not in SOURCES:
        logger.error(f"[poller] Неизвестный источник: {source_name}")
        return

    module_path = SOURCES[source_name]
    import importlib
    module = importlib.import_module(module_path)

    ensure_cursor_table()
    since = get_cursor(source_name)

    # Первый запуск (cursor не был установлен) — берём только за последние days_back дней
    first_run = since.year == 1970
    if first_run:
        since = datetime.now() - timedelta(days=days_back)
        logger.info(f"[{source_name}] Первый запуск — берём за последние {days_back} дн. (с {since:%Y-%m-%d})")

    processed_ids = get_processed_ids(source_name)

    logger.info(f"[{source_name}] cursor={since} | уже обработано={len(processed_ids)}")

    dialogs = module.fetch_finished_dialogs(since=since)
    new_dialogs = [d for d in dialogs if d.chat_id not in processed_ids]

    if limit:
        new_dialogs = new_dialogs[:limit]

    logger.info(f"[{source_name}] Найдено новых: {len(new_dialogs)}")

    if dry_run:
        for d in new_dialogs:
            print(f"  [{d.source}] id={d.dialog_id} | {d.visitor_name} → {d.operator_name} | {d.finished_at}")
            print(f"    {d.plain_messages[:120].replace(chr(10), ' ')}...")
        return

    ok = fail = 0
    max_ts = since

    for i, dialog in enumerate(new_dialogs, 1):
        logger.info(f"[{source_name}] [{i}/{len(new_dialogs)}] dialog_id={dialog.dialog_id}")
        try:
            save_raw(dialog)
            save_dialog(dialog)
            analyze_and_save(dialog.to_payload())
            ok += 1
            ts = datetime.fromisoformat(dialog.finished_at)
            if ts > max_ts:
                max_ts = ts
        except Exception as e:
            logger.error(f"[{source_name}] Ошибка dialog_id={dialog.dialog_id}: {e}")
            fail += 1
        if i < len(new_dialogs):
            time.sleep(0.5)

    if max_ts > since:
        set_cursor(source_name, max_ts)

    logger.info(f"[{source_name}] Готово: {ok} успешно, {fail} ошибок")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=None,
                        help=f"источник: {', '.join(SOURCES)} (по умолчанию все)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--days-back", type=int, default=1,
                        help="при первом запуске — сколько дней истории брать (default: 1)")
    args = parser.parse_args()

    sources = [args.source] if args.source else list(SOURCES.keys())

    for src in sources:
        run_source(src, dry_run=args.dry_run, limit=args.limit, days_back=args.days_back)


if __name__ == "__main__":
    main()
