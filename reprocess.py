"""
Перепрогон диалогов из raw_jivo_chat через AI.
Пропускает chat_id, которые уже есть в jivo_chat_analysis.

Запуск:
    python3 reprocess.py [--dry-run] [--limit N] [--force]

--dry-run  не отправлять в AI, только показать что будет обработано
--limit N  обработать не более N диалогов
--force    обрабатывать даже те, что уже есть в jivo_chat_analysis
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


def load_dotenv(path: str = "/opt/jivo_inspector/.env"):
    """Загружает .env файл в os.environ, корректно обрабатывает значения с пробелами."""
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        os.environ[key.strip()] = value  # всегда перезаписываем


# Загружаем .env ДО импорта ai_processor
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from ai_processor import analyze_and_save, CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE


def ch_query_rows(sql: str) -> list:
    """Выполняет SELECT и возвращает список строк (JSONEachRow)."""
    params = urllib.parse.urlencode({
        "query":    sql,
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=30)
    rows = []
    for line in resp.read().decode().strip().splitlines():
        if line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def get_already_processed() -> set:
    """Возвращает set chat_id из jivo_chat_analysis."""
    try:
        rows = ch_query_rows(
            "SELECT DISTINCT chat_id FROM jivo_chat_analysis FORMAT JSONEachRow"
        )
        return {int(r["chat_id"]) for r in rows}
    except Exception as e:
        print(f"[warn] Не удалось получить обработанные chat_id: {e}")
        return set()


def load_dialogs() -> list:
    """Загружает все chat_finished диалоги из raw_jivo_chat."""
    try:
        rows = ch_query_rows(
            "SELECT chat_id, payload_json FROM raw_jivo_chat "
            "WHERE event_name = 'chat_finished' "
            "ORDER BY received_at ASC "
            "FORMAT JSONEachRow"
        )
        result = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
                chat_id = int(row["chat_id"])
                result.append((chat_id, payload))
            except Exception as e:
                print(f"[skip] chat_id={row.get('chat_id')}: {e}")
        return result
    except Exception as e:
        print(f"[error] Не удалось загрузить диалоги: {e}")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print("Загружаем уже обработанные диалоги из jivo_chat_analysis...")
    already_done = set() if args.force else get_already_processed()
    print(f"Уже обработано: {len(already_done)}\n")

    print("Загружаем диалоги из raw_jivo_chat...")
    all_dialogs = load_dialogs()
    print(f"Найдено chat_finished: {len(all_dialogs)}")

    to_process = [
        (chat_id, payload)
        for chat_id, payload in all_dialogs
        if chat_id not in already_done
    ]

    if args.limit:
        to_process = to_process[:args.limit]

    print(f"К обработке: {len(to_process)}\n")

    if args.dry_run:
        print("=== DRY RUN ===")
        for i, (chat_id, payload) in enumerate(to_process, 1):
            visitor = payload.get("visitor") or {}
            print(f"  {i}. chat_id={chat_id} | visitor={visitor.get('name', '?')}")
        return

    ok = 0
    fail = 0
    for i, (chat_id, payload) in enumerate(to_process, 1):
        print(f"[{i}/{len(to_process)}] chat_id={chat_id}")
        try:
            analyze_and_save(payload)
            ok += 1
        except Exception as e:
            print(f"  [ERROR] {e}")
            fail += 1
        if i < len(to_process):
            time.sleep(0.5)

    print(f"\n=== Готово: {ok} успешно, {fail} ошибок ===")


if __name__ == "__main__":
    main()
