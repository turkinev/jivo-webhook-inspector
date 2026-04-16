"""
SLA-чекер: считает скорость первого ответа и фиксирует нарушения SLA.
Записывает результаты в таблицу dialog_sla (ClickHouse).

Режимы:
    --mode completed   обработать завершённые диалоги (по умолчанию)
    --mode open        найти открытые треды с просроченным SLA (MySQL)

Фильтры:
    --source jivo|site_pm   только один источник (по умолчанию оба)
    --days-back N           сколько дней истории обрабатывать (default 1)
    --dry-run               только вывод, без записи в CH

Запуск:
    python3 sla_checker.py --mode completed
    python3 sla_checker.py --mode open
"""

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Optional


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

# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

PM_DB_HOST     = os.getenv("PM_DB_HOST", "10.1.100.61")
PM_DB_PORT     = int(os.getenv("PM_DB_PORT", "3306"))
PM_DB_USER     = os.getenv("PM_DB_USER", "dev")
PM_DB_PASSWORD = os.getenv("PM_DB_PASSWORD", "dev")
PM_DB_NAME     = os.getenv("PM_DB_NAME", "msg")
PM_USER_DB     = os.getenv("PM_USER_DB", "user")

# SLA site_pm: рабочие часы Mon-Fri 9:00-18:00 (серверное время)
SLA_START_HOUR    = int(os.getenv("SLA_START_HOUR", "9"))
SLA_END_HOUR      = int(os.getenv("SLA_END_HOUR", "18"))
SLA_RESPONSE_HOUR = int(os.getenv("SLA_RESPONSE_HOUR", "11"))  # дедлайн на след.день

# SLA Jivo: 5 мин в рабочее время 8:00-20:00, часовой пояс Самара (UTC+4)
JIVO_SLA_MINUTES    = int(os.getenv("JIVO_SLA_MINUTES", "5"))
JIVO_SLA_START_HOUR = int(os.getenv("JIVO_SLA_START_HOUR", "8"))
JIVO_SLA_END_HOUR   = int(os.getenv("JIVO_SLA_END_HOUR", "20"))
JIVO_TZ_OFFSET      = int(os.getenv("JIVO_TZ_OFFSET_HOURS", "4"))  # UTC+4 Самара

# Инактивность треда для site_pm (должно совпадать с коннектором)
PM_INACTIVITY_HOURS = int(os.getenv("PM_INACTIVITY_HOURS", "12"))

# Часовой пояс Самара (UTC+4) — без зависимостей
_TZ_SAMARA = timezone(timedelta(hours=JIVO_TZ_OFFSET))


def ch_exec(sql: str, data: bytes = None) -> str:
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
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"CH {e.code}: {e.read().decode()[:500]}") from None


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_SLA_TABLE = """
CREATE TABLE IF NOT EXISTS dialog_sla (
    source           LowCardinality(String),
    chat_id          UInt64,
    client_msg_at    DateTime,
    operator_msg_at  Nullable(DateTime),
    sla_deadline     DateTime,
    response_minutes Nullable(Int32),
    sla_violated     UInt8,
    is_open          UInt8,
    calculated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (source, chat_id)
"""


def ensure_table():
    ch_exec(CREATE_SLA_TABLE)


# ---------------------------------------------------------------------------
# SLA deadline logic
# ---------------------------------------------------------------------------

def _next_workday(dt: datetime) -> datetime:
    """Возвращает следующий рабочий день (Пн-Пт), naive datetime."""
    next_day = dt + timedelta(days=1)
    while next_day.weekday() >= 5:  # 5=Sat, 6=Sun
        next_day += timedelta(days=1)
    return next_day


def sla_deadline(received_at: datetime) -> datetime:
    """
    Дедлайн SLA для site_pm (серверное время, рабочие часы Mon-Fri 9:00-18:00).

    - Пн-Пт 9:00-18:00 → дедлайн 18:00 того же дня
    - Пн-Пт до 9:00   → дедлайн 18:00 того же дня
    - Пн-Пт после 18:00 → 11:00 следующего рабочего дня
    - Пт после 18:00 / Сб / Вс → 11:00 понедельника
    """
    wd = received_at.weekday()  # 0=Mon … 6=Sun
    h  = received_at.hour

    if wd < 5 and h < SLA_END_HOUR:
        # В рабочий день и до конца рабочего времени — дедлайн сегодня в 18:00
        return received_at.replace(hour=SLA_END_HOUR, minute=0, second=0, microsecond=0)

    # После рабочего времени или выходной — следующий рабочий день 11:00
    next_wd = _next_workday(received_at)
    return next_wd.replace(hour=SLA_RESPONSE_HOUR, minute=0, second=0, microsecond=0)


def sla_deadline_jivo(received_at: datetime) -> datetime:
    """
    Дедлайн SLA для Jivo (Самара UTC+4, рабочие часы Mon-Fri 8:00-20:00).

    В рабочее время:
        дедлайн = received_at + JIVO_SLA_MINUTES минут

    Вне рабочего времени / выходные:
        дедлайн = начало следующего рабочего периода + JIVO_SLA_MINUTES минут
        (т.е. клиент, написавший в 21:00 пт, получит ответ до 08:05 пн)

    Входящее время может быть naive (считается серверным временем) или aware.
    Для расчёта рабочего окна всегда переводим в Самарское время.
    """
    # Приводим к Самаре
    if received_at.tzinfo is None:
        # Считаем, что сервер хранит UTC — переводим в Самару
        # Если сервер уже в UTC+4, поменяйте на: samara_dt = received_at
        samara_dt = received_at + timedelta(hours=JIVO_TZ_OFFSET)
    else:
        samara_dt = received_at.astimezone(_TZ_SAMARA).replace(tzinfo=None)

    wd = samara_dt.weekday()  # 0=Mon … 6=Sun
    h  = samara_dt.hour

    in_working_hours = wd < 5 and JIVO_SLA_START_HOUR <= h < JIVO_SLA_END_HOUR

    if in_working_hours:
        deadline_samara = samara_dt + timedelta(minutes=JIVO_SLA_MINUTES)
        # Если дедлайн выходит за 20:00 — переносим на следующий рабочий день 8:00+5мин
        if deadline_samara.hour >= JIVO_SLA_END_HOUR:
            next_wd = _next_workday(samara_dt)
            deadline_samara = next_wd.replace(
                hour=JIVO_SLA_START_HOUR, minute=JIVO_SLA_MINUTES, second=0, microsecond=0
            )
    else:
        # Вне рабочего времени — следующий рабочий день 8:00 + 5 мин
        next_wd = _next_workday(samara_dt) if (wd >= 5 or h >= JIVO_SLA_END_HOUR) else samara_dt
        if wd < 5 and h < JIVO_SLA_START_HOUR:
            # До начала рабочего дня — дедлайн сегодня 8:05
            next_wd = samara_dt
        deadline_samara = next_wd.replace(
            hour=JIVO_SLA_START_HOUR, minute=JIVO_SLA_MINUTES, second=0, microsecond=0
        )

    # Возвращаем в серверное время (naive UTC, если сервер UTC)
    if received_at.tzinfo is None:
        return deadline_samara - timedelta(hours=JIVO_TZ_OFFSET)
    else:
        return deadline_samara.replace(tzinfo=_TZ_SAMARA).astimezone(
            received_at.tzinfo
        ).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Запись SLA-строки в CH
# ---------------------------------------------------------------------------

def write_sla_row(
    source: str,
    chat_id: int,
    client_msg_at: datetime,
    operator_msg_at: Optional[datetime],
    sla_dl: datetime,
    is_open: int,
    dry_run: bool,
):
    if operator_msg_at is not None:
        response_minutes = int((operator_msg_at - client_msg_at).total_seconds() / 60)
        sla_violated = 1 if operator_msg_at > sla_dl else 0
    else:
        response_minutes = None
        # Открытый тред — нарушение если уже просрочен
        sla_violated = 1 if datetime.now() > sla_dl else 0

    if dry_run:
        status = "OPEN" if is_open else ("VIOLATED" if sla_violated else "OK")
        op_str = operator_msg_at.isoformat() if operator_msg_at else "—"
        print(
            f"  [{source}] chat={chat_id} | client={client_msg_at:%H:%M} "
            f"op={op_str} deadline={sla_dl:%Y-%m-%d %H:%M} "
            f"resp={response_minutes}min [{status}]"
        )
        return

    row = {
        "source":           source,
        "chat_id":          chat_id,
        "client_msg_at":    client_msg_at.isoformat(),
        "operator_msg_at":  operator_msg_at.isoformat() if operator_msg_at else None,
        "sla_deadline":     sla_dl.isoformat(),
        "response_minutes": response_minutes,
        "sla_violated":     sla_violated,
        "is_open":          is_open,
    }
    # Удаляем None-поля — CH вставит NULL сам
    row = {k: v for k, v in row.items() if v is not None}
    ch_exec(
        "INSERT INTO dialog_sla FORMAT JSONEachRow",
        data=json.dumps(row, ensure_ascii=False).encode(),
    )


# ---------------------------------------------------------------------------
# Источник: Jivo (chat_messages_json из dialogs)
# ---------------------------------------------------------------------------

def process_jivo_completed(since: datetime, dry_run: bool):
    """
    Обрабатывает завершённые Jivo-диалоги из таблицы dialogs.
    Первое сообщение visitor_a → дедлайн. Первый ответ agent → response_minutes.
    """
    sql = f"""
        SELECT
            d.chat_id,
            d.chat_messages_json,
            d.event_timestamp
        FROM dialogs d
        LEFT JOIN dialog_sla s FINAL ON s.source = 'jivo' AND s.chat_id = d.chat_id
        WHERE d.source = 'jivo'
          AND d.event_timestamp >= '{since:%Y-%m-%d %H:%M:%S}'
          AND s.chat_id = 0  -- не обработан ранее
        ORDER BY d.event_timestamp ASC
        LIMIT 5000
        FORMAT JSONEachRow
    """
    # Используем FINAL-фильтрацию через LEFT JOIN нет — используем NOT IN
    sql = f"""
        SELECT
            d.chat_id,
            d.chat_messages_json,
            d.event_timestamp
        FROM dialogs d
        WHERE d.source = 'jivo'
          AND d.event_timestamp >= '{since:%Y-%m-%d %H:%M:%S}'
          AND d.chat_id NOT IN (
              SELECT chat_id FROM dialog_sla FINAL WHERE source = 'jivo'
          )
        ORDER BY d.event_timestamp ASC
        LIMIT 5000
        FORMAT JSONEachRow
    """

    result = ch_exec(sql).strip()
    if not result:
        logger.info("[sla][jivo] Нет новых диалогов")
        return

    rows = [json.loads(line) for line in result.splitlines() if line]
    logger.info(f"[sla][jivo] Диалогов к обработке: {len(rows)}")

    ok = skip = 0
    for row in rows:
        chat_id = int(row["chat_id"])
        messages_json = row.get("chat_messages_json") or "[]"
        event_ts = datetime.fromisoformat(row["event_timestamp"])

        try:
            messages = json.loads(messages_json)
        except Exception:
            skip += 1
            continue

        if not messages:
            skip += 1
            continue

        # Ищем первое сообщение клиента и первый ответ оператора
        client_msg_at = None
        operator_msg_at = None

        for msg in messages:
            sender = str(msg.get("type", msg.get("sender_type", ""))).lower()
            # visitor / client / user — сообщение клиента
            # agent / operator — ответ оператора
            is_client = sender in ("visitor", "client", "user", "visitor_a")
            is_agent  = sender in ("agent", "operator", "agent_a")

            # Время: поле timestamp (unix) или created_at (iso)
            ts = None
            if "timestamp" in msg:
                try:
                    ts = datetime.fromtimestamp(int(msg["timestamp"]))
                except Exception:
                    pass
            if ts is None and "created_at" in msg:
                try:
                    ts = datetime.fromisoformat(str(msg["created_at"]))
                except Exception:
                    pass

            if ts is None:
                continue

            if is_client and client_msg_at is None:
                client_msg_at = ts
            elif is_agent and client_msg_at is not None and operator_msg_at is None:
                operator_msg_at = ts

        if client_msg_at is None:
            skip += 1
            continue

        sla_dl = sla_deadline_jivo(client_msg_at)
        write_sla_row("jivo", chat_id, client_msg_at, operator_msg_at, sla_dl, 0, dry_run)
        ok += 1

    logger.info(f"[sla][jivo] Записано: {ok}, пропущено: {skip}")


# ---------------------------------------------------------------------------
# Источник: site_pm — завершённые
# ---------------------------------------------------------------------------

def _get_pm_conn():
    try:
        import pymysql
        return pymysql.connect(
            host=PM_DB_HOST, port=PM_DB_PORT,
            user=PM_DB_USER, password=PM_DB_PASSWORD,
            database=PM_DB_NAME,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except ImportError:
        raise RuntimeError("pymysql не установлен: pip install pymysql")


def _load_org_ids(conn) -> set:
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT user_id FROM {PM_USER_DB}.user_group
            WHERE group_id = 58
              AND user_id NOT IN (
                  SELECT user_id FROM {PM_USER_DB}.user_group
                  WHERE group_id IN (4, 5)
              )
        """)
        return {r["user_id"] for r in cur.fetchall()}
    except Exception as e:
        logger.warning(f"[sla][site_pm] Не удалось загрузить org_ids: {e}")
        return set()


def process_pm_completed(since: datetime, dry_run: bool):
    """
    Обрабатывает завершённые треды site_pm из MySQL.

    Завершённый = последнее сообщение > 1ч назад
                  И оператор прочитал тред (thread_user.is_unread = 0).

    Первое сообщение клиента → дедлайн SLA.
    Первый ответ организатора → response_minutes.
    """
    # Уже обработанные chat_id
    try:
        processed = set()
        result = ch_exec(
            "SELECT DISTINCT chat_id FROM dialog_sla FINAL WHERE source = 'site_pm' FORMAT JSONEachRow"
        ).strip()
        if result:
            processed = {int(json.loads(l)["chat_id"]) for l in result.splitlines() if l}
    except Exception as e:
        logger.warning(f"[sla][site_pm] Не удалось загрузить processed_ids: {e}")
        processed = set()

    deadline_ts = datetime.now() - timedelta(hours=PM_INACTIVITY_HOURS)

    conn = _get_pm_conn()
    org_ids = _load_org_ids(conn)

    if not org_ids:
        logger.warning("[sla][site_pm] Список организаторов пуст")
        conn.close()
        return

    org_list = list(org_ids)
    org_ph   = ",".join(["%s"] * len(org_list))

    try:
        cur = conn.cursor()

        # Берём только треды, где оператор прочитал (is_unread=0)
        cur.execute(f"""
            SELECT t.id AS thread_id
            FROM thread t
            JOIN message m ON m.thread_id = t.id AND m.is_deleted = 0
            WHERE EXISTS (
                SELECT 1 FROM thread_user tu
                WHERE tu.thread_id = t.id
                  AND tu.user_id IN ({org_ph})
                  AND tu.is_unread = 0
                  AND (tu.is_deleted = 0 OR tu.is_deleted IS NULL)
            )
            GROUP BY t.id
            HAVING MAX(m.created_at) <= %s
               AND MAX(m.created_at) >  %s
            ORDER BY MAX(m.created_at) ASC
            LIMIT 5000
        """, (*org_list, deadline_ts, since))

        rows = cur.fetchall()
        logger.info(f"[sla][site_pm] Завершённых тредов к обработке: {len(rows)}")

        ok = skip = 0
        for row in rows:
            thread_id = row["thread_id"]
            if thread_id in processed:
                skip += 1
                continue

            try:
                cur2 = conn.cursor()
                cur2.execute("""
                    SELECT author_user_id, created_at
                    FROM message
                    WHERE thread_id = %s AND is_deleted = 0
                    ORDER BY created_at ASC
                """, (thread_id,))
                messages = cur2.fetchall()

                if not messages:
                    skip += 1
                    continue

                client_msg_at   = None
                operator_msg_at = None

                for msg in messages:
                    uid = msg["author_user_id"]
                    ts  = msg["created_at"]
                    if isinstance(ts, str):
                        ts = datetime.fromisoformat(ts)

                    if uid not in org_ids and client_msg_at is None:
                        client_msg_at = ts
                    elif uid in org_ids and client_msg_at is not None and operator_msg_at is None:
                        operator_msg_at = ts

                if client_msg_at is None:
                    skip += 1
                    continue

                sla_dl = sla_deadline(client_msg_at)
                write_sla_row("site_pm", thread_id, client_msg_at, operator_msg_at, sla_dl, 0, dry_run)
                ok += 1

            except Exception as e:
                logger.error(f"[sla][site_pm] Ошибка thread_id={thread_id}: {e}")
                skip += 1

        logger.info(f"[sla][site_pm] Записано: {ok}, пропущено: {skip}")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Режим open: брошенные треды — is_unread=1 у оператора + SLA просрочен
# ---------------------------------------------------------------------------

def process_pm_open(dry_run: bool):
    """
    Ищет брошенные треды: оператор не прочитал/не ответил (is_unread=1)
    и SLA дедлайн уже истёк.

    SLA правила:
      Пн-Пт 9:00-18:00 → дедлайн 18:00 того же дня
      После 18:00       → 11:00 следующего рабочего дня
      После 18:00 сб    → 11:00 понедельника

    Записывает is_open=1 в dialog_sla.
    """
    conn = _get_pm_conn()
    org_ids = _load_org_ids(conn)

    if not org_ids:
        logger.warning("[sla][open] Список организаторов пуст")
        conn.close()
        return

    org_list = list(org_ids)
    org_ph   = ",".join(["%s"] * len(org_list))

    try:
        cur = conn.cursor()

        # Треды за последние 7 дней, где оператор НЕ прочитал (is_unread=1)
        active_since = datetime.now() - timedelta(days=7)

        cur.execute(f"""
            SELECT
                t.id AS thread_id,
                MIN(CASE WHEN m.author_user_id NOT IN ({org_ph}) THEN m.created_at END)
                    AS first_client_msg_at
            FROM thread t
            JOIN message m ON m.thread_id = t.id AND m.is_deleted = 0
            WHERE t.created_at >= %s
              AND EXISTS (
                  SELECT 1 FROM thread_user tu
                  WHERE tu.thread_id = t.id
                    AND tu.user_id IN ({org_ph})
                    AND tu.is_unread = 1
                    AND (tu.is_deleted = 0 OR tu.is_deleted IS NULL)
              )
            GROUP BY t.id
            HAVING first_client_msg_at IS NOT NULL
            ORDER BY first_client_msg_at ASC
            LIMIT 2000
        """, (*org_list, active_since, *org_list))

        rows = cur.fetchall()
        logger.info(f"[sla][open] Непрочитанных тредов: {len(rows)}")

        overdue = ok = 0
        for row in rows:
            thread_id         = row["thread_id"]
            first_client_msg  = row["first_client_msg_at"]

            if first_client_msg is None:
                continue
            if isinstance(first_client_msg, str):
                first_client_msg = datetime.fromisoformat(first_client_msg)

            sla_dl = sla_deadline(first_client_msg)

            if datetime.now() <= sla_dl:
                continue  # SLA ещё не истёк — всё в порядке

            try:
                write_sla_row("site_pm", thread_id, first_client_msg, None, sla_dl, 1, dry_run)
                overdue += 1
            except Exception as e:
                logger.error(f"[sla][open] Ошибка thread_id={thread_id}: {e}")

        logger.info(f"[sla][open] Просроченных без ответа: {overdue} из {len(rows)}")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["completed", "open"], default="completed",
                        help="completed — завершённые диалоги; open — активные без ответа")
    parser.add_argument("--source", choices=["jivo", "site_pm", "all"], default="all")
    parser.add_argument("--days-back", type=int, default=1,
                        help="сколько дней истории обрабатывать (default 1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="только вывод, без записи в CH")
    args = parser.parse_args()

    ensure_table()

    since = datetime.now() - timedelta(days=args.days_back)
    logger.info(f"[sla] mode={args.mode} source={args.source} since={since:%Y-%m-%d} dry_run={args.dry_run}")

    if args.mode == "completed":
        if args.source in ("jivo", "all"):
            process_jivo_completed(since, args.dry_run)
        if args.source in ("site_pm", "all"):
            process_pm_completed(since, args.dry_run)

    elif args.mode == "open":
        if args.source in ("site_pm", "all"):
            process_pm_open(args.dry_run)
        if args.source == "jivo":
            logger.info("[sla][open] Для Jivo open-режим не поддерживается (нет live-данных)")


if __name__ == "__main__":
    main()
