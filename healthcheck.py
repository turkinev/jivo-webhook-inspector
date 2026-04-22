"""
Проверка работоспособности сервиса. Запускается по cron каждый час.
Отправляет уведомление в Mattermost если что-то не так.

Проверки:
  1. Сервис /health отвечает
  2. ClickHouse доступен
  3. Последний chat_finished от Jivo не старше 2 часов (Пн-Пт 8:00-20:00 Самара)

Запуск:
    python3 healthcheck.py [--dry-run]
"""

import argparse
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

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

# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------

CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

SERVICE_URL = os.getenv("SERVICE_URL", "http://localhost:62000")

MM_WEBHOOK  = os.getenv("MM_WEBHOOK", "https://mm.63pokupki.ru:8443/hooks/beejdsmu83y7tyf5yodzp6dqpa")

# Jivo: рабочее время Самара UTC+4, Пн-Пт 8:00-20:00
JIVO_TZ_OFFSET      = int(os.getenv("JIVO_TZ_OFFSET_HOURS", "4"))
JIVO_WORK_START     = int(os.getenv("JIVO_SLA_START_HOUR", "8"))
JIVO_WORK_END       = int(os.getenv("JIVO_SLA_END_HOUR", "20"))
JIVO_MAX_SILENCE_H  = int(os.getenv("JIVO_MAX_SILENCE_HOURS", "2"))  # макс. часов без chat_finished


# ---------------------------------------------------------------------------
# Проверки
# ---------------------------------------------------------------------------

def check_service() -> Tuple[bool, str]:
    """Проверяет /health endpoint."""
    try:
        req = urllib.request.Request(f"{SERVICE_URL}/health")
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read().decode())
        if data.get("status") != "ok":
            return False, f"status={data.get('status')}"
        if data.get("clickhouse") != "ok":
            return False, f"clickhouse={data.get('clickhouse')}"
        return True, "ok"
    except Exception as e:
        return False, str(e)


def check_jivo_silence() -> Tuple[bool, str]:
    """
    Проверяет что последний chat_finished от Jivo не старше JIVO_MAX_SILENCE_H часов.
    Проверка активна только в рабочее время Пн-Пт 8:00-20:00 (Самара UTC+4).
    """
    # Текущее время в Самаре
    now_samara = datetime.utcnow() + timedelta(hours=JIVO_TZ_OFFSET)
    wd = now_samara.weekday()  # 0=Пн, 6=Вс
    h  = now_samara.hour

    # Вне рабочего времени — не проверяем
    if wd >= 5 or not (JIVO_WORK_START <= h < JIVO_WORK_END):
        return True, "вне рабочего времени — пропускаем"

    # Grace period: с начала рабочего дня ещё не прошло JIVO_MAX_SILENCE_H часов
    minutes_since_start = (h - JIVO_WORK_START) * 60 + now_samara.minute
    if minutes_since_start < JIVO_MAX_SILENCE_H * 60:
        return True, f"grace period — рабочий день начался {minutes_since_start} мин назад"

    try:
        params = urllib.parse.urlencode({
            "user": CH_USER, "password": CH_PASSWORD, "database": CH_DATABASE,
        })
        sql = "SELECT max(received_at) AS last FROM raw_dialogs WHERE source='jivo' AND event_name='chat_finished' FORMAT JSONEachRow"
        url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
        req = urllib.request.Request(url, data=sql.encode(), method="POST")
        resp = urllib.request.urlopen(req, timeout=10)
        result = resp.read().decode().strip()

        if not result:
            return False, "нет ни одного chat_finished в базе"

        last_str = json.loads(result).get("last", "")
        if not last_str or last_str.startswith("1970"):
            return False, "нет данных о последнем chat_finished"

        last_dt = datetime.fromisoformat(last_str)
        silence = datetime.now() - last_dt
        hours   = silence.total_seconds() / 3600

        if hours > JIVO_MAX_SILENCE_H:
            return False, f"последний chat_finished {hours:.1f}ч назад (лимит {JIVO_MAX_SILENCE_H}ч)"

        return True, f"последний chat_finished {hours:.1f}ч назад"

    except Exception as e:
        return False, f"ошибка запроса к CH: {e}"


# ---------------------------------------------------------------------------
# Mattermost
# ---------------------------------------------------------------------------

def send_mattermost(text: str, dry_run: bool = False):
    if dry_run:
        print(f"\n[Mattermost уведомление]\n{text}\n")
        return
    try:
        body = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
        req  = urllib.request.Request(
            MM_WEBHOOK,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        logger.info("[healthcheck] Уведомление отправлено в Mattermost")
    except Exception as e:
        logger.error(f"[healthcheck] Ошибка отправки в Mattermost: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="не отправлять в Mattermost, вывести в консоль")
    args = parser.parse_args()

    errors = []

    # 1. Сервис
    ok, msg = check_service()
    logger.info(f"[healthcheck] service: {'OK' if ok else 'FAIL'} — {msg}")
    if not ok:
        errors.append(f"🔴 *Сервис недоступен*: `{msg}`")

    # 2. Тишина от Jivo
    ok, msg = check_jivo_silence()
    logger.info(f"[healthcheck] jivo_silence: {'OK' if ok else 'WARN'} — {msg}")
    if not ok:
        errors.append(f"⚠️ *Jivo не присылает chat_finished*: {msg}")

    if not errors:
        logger.info("[healthcheck] Всё в порядке")
        return

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    text = f"@channel\n**⚠️ Dialog Analytics — проблема [{now}]**\n\n" + "\n".join(errors)
    send_mattermost(text, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
