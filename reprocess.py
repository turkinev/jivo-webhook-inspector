"""
Перепрогон диалогов из raw_dialogs через AI.
Пропускает chat_id, которые уже есть в dialog_analysis.

Запуск:
    python3 reprocess.py [--dry-run] [--limit N] [--force] [--source SOURCE]

--dry-run        не отправлять в AI, только показать что будет обработано
--limit N        обработать не более N диалогов
--force          обрабатывать даже те, что уже есть в dialog_analysis
--source SOURCE  только конкретный источник (jivo, site_pm, ...)
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
    """Выполняет SELECT через POST и возвращает список строк (JSONEachRow)."""
    params = urllib.parse.urlencode({
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    resp = urllib.request.urlopen(req, timeout=30)
    rows = []
    for line in resp.read().decode().strip().splitlines():
        if line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def get_already_processed(source: str = None) -> set:
    """Возвращает set chat_id из dialog_analysis (опционально по источнику)."""
    try:
        where = f"WHERE source = '{source}'" if source else ""
        rows = ch_query_rows(
            f"SELECT DISTINCT chat_id FROM dialog_analysis {where} FORMAT JSONEachRow"
        )
        return {int(r["chat_id"]) for r in rows}
    except Exception as e:
        print(f"[warn] Не удалось получить обработанные chat_id: {e}")
        return set()


def load_dialogs(source: str = None) -> list:
    """Загружает все chat_finished диалоги из raw_dialogs."""
    try:
        where = "WHERE event_name = 'chat_finished'"
        if source:
            where += f" AND source = '{source}'"
        rows = ch_query_rows(
            f"SELECT source, chat_id, payload_json FROM raw_dialogs "
            f"{where} "
            f"ORDER BY received_at ASC "
            f"FORMAT JSONEachRow"
        )
        result = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
                payload["source"] = row.get("source", "jivo")
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
    parser.add_argument("--source", type=str, default=None,
                        help="фильтр по источнику: jivo, site_pm, ...")
    args = parser.parse_args()

    source_label = args.source or "все источники"
    print(f"Загружаем уже обработанные диалоги из dialog_analysis [{source_label}]...")
    already_done = set() if args.force else get_already_processed(args.source)
    print(f"Уже обработано: {len(already_done)}\n")

    print(f"Загружаем диалоги из raw_dialogs [{source_label}]...")
    all_dialogs = load_dialogs(args.source)
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
            src = payload.get("source", "?")
            print(f"  {i}. [{src}] chat_id={chat_id} | visitor={visitor.get('name', '?')}")
        return

    ok = 0
    fail = 0
    for i, (chat_id, payload) in enumerate(to_process, 1):
        src = payload.get("source", "?")
        print(f"[{i}/{len(to_process)}] [{src}] chat_id={chat_id}")
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
