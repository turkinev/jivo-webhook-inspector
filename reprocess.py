"""
Перепрогон сохранённых chat_finished через AI.
Пропускает chat_id, которые уже есть в jivo_chat_analysis.

Запуск:
    python3 reprocess.py [--dry-run] [--limit N] [--force]

--dry-run  не отправлять в AI, только показать что будет обработано
--limit N  обработать не более N диалогов
--force    обрабатывать даже те, что уже есть в jivo_chat_analysis
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# Подгружаем конфиг и процессор из той же папки
sys.path.insert(0, str(Path(__file__).parent))
from ai_processor import analyze_and_save, CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE

LOG_DIR = Path("logs")


def ch_query(sql: str) -> list:
    params = urllib.parse.urlencode({
        "query":    sql,
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=10)
    lines = resp.read().decode().strip().splitlines()
    return [line for line in lines if line]


def get_already_processed() -> set:
    """Возвращает set chat_id, которые уже есть в jivo_chat_analysis."""
    try:
        rows = ch_query("SELECT DISTINCT chat_id FROM jivo_chat_analysis")
        return {int(r) for r in rows if r.isdigit()}
    except Exception as e:
        print(f"[warn] Не удалось получить обработанные chat_id: {e}")
        return set()


def load_finished_dialogs() -> list[tuple[int, dict, Path]]:
    """Загружает все chat_finished файлы из logs/. Возвращает (chat_id, payload, path)."""
    files = sorted(LOG_DIR.glob("*_chat_finished.json"))
    result = []
    for f in files:
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
            chat_id = int(payload.get("chat_id") or 0)
            if chat_id > 0:
                result.append((chat_id, payload, f))
        except Exception as e:
            print(f"[skip] {f.name}: {e}")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Только показать, не обрабатывать")
    parser.add_argument("--limit", type=int, default=0, help="Лимит диалогов (0 = все)")
    parser.add_argument("--force", action="store_true", help="Обрабатывать даже уже обработанные")
    args = parser.parse_args()

    print("Загружаем список уже обработанных диалогов из CH...")
    already_done = set() if args.force else get_already_processed()
    print(f"Уже в jivo_chat_analysis: {len(already_done)} диалогов\n")

    print(f"Читаем файлы из {LOG_DIR}/...")
    all_dialogs = load_finished_dialogs()
    print(f"Найдено chat_finished файлов: {len(all_dialogs)}")

    to_process = [
        (chat_id, payload, path)
        for chat_id, payload, path in all_dialogs
        if chat_id not in already_done
    ]

    if args.limit:
        to_process = to_process[:args.limit]

    print(f"К обработке: {len(to_process)}\n")

    if args.dry_run:
        print("=== DRY RUN — ничего не отправляется ===")
        for i, (chat_id, payload, path) in enumerate(to_process, 1):
            visitor = payload.get("visitor") or {}
            print(f"  {i}. chat_id={chat_id} | {path.name} | visitor={visitor.get('name', '?')}")
        return

    ok = 0
    fail = 0
    for i, (chat_id, payload, path) in enumerate(to_process, 1):
        print(f"[{i}/{len(to_process)}] chat_id={chat_id} | {path.name}")
        try:
            analyze_and_save(payload)
            ok += 1
        except Exception as e:
            print(f"  [ERROR] {e}")
            fail += 1
        # Небольшая пауза чтобы не спамить AI API
        if i < len(to_process):
            time.sleep(0.5)

    print(f"\n=== Готово: {ok} успешно, {fail} ошибок ===")


if __name__ == "__main__":
    main()
