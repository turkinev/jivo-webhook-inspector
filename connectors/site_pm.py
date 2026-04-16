"""
Коннектор: личные сообщения сайта (MySQL).

Источник данных:
  host: 10.1.100.61  db: msg  user: dev
  thread, message, thread_user  (база msg)
  user.user, user.user_orgrole  (база user)

Логика завершённости:
  Диалог считается завершённым если последнее сообщение было
  более INACTIVITY_HOURS часов назад.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Настройки из env
# ---------------------------------------------------------------------------

PM_DB_HOST     = os.getenv("PM_DB_HOST", "10.1.100.61")
PM_DB_PORT     = int(os.getenv("PM_DB_PORT", "3306"))
PM_DB_USER     = os.getenv("PM_DB_USER", "dev")
PM_DB_PASSWORD = os.getenv("PM_DB_PASSWORD", "dev")
PM_DB_NAME     = os.getenv("PM_DB_NAME", "msg")
PM_USER_DB     = os.getenv("PM_USER_DB", "user")

INACTIVITY_HOURS = int(os.getenv("PM_INACTIVITY_HOURS", "12"))


def _get_conn():
    """Открывает соединение с MySQL (требует pymysql)."""
    try:
        import pymysql
        return pymysql.connect(
            host=PM_DB_HOST,
            port=PM_DB_PORT,
            user=PM_DB_USER,
            password=PM_DB_PASSWORD,
            database=PM_DB_NAME,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except ImportError:
        raise RuntimeError("pymysql не установлен: pip install pymysql")


# ---------------------------------------------------------------------------
# Кэш организаторов (чтобы не запрашивать каждый раз)
# ---------------------------------------------------------------------------

_org_user_ids: Optional[set] = None


def _load_org_ids(conn) -> set:
    """Загружает set user_id организаторов: группа 58, исключая группы 4 и 5."""
    global _org_user_ids
    if _org_user_ids is not None:
        return _org_user_ids
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
        _org_user_ids = {r["user_id"] for r in cur.fetchall()}
        logger.info(f"[site_pm] Загружено {len(_org_user_ids)} организаторов")
    except Exception as e:
        logger.warning(f"[site_pm] Не удалось загрузить организаторов: {e}")
        _org_user_ids = set()
    return _org_user_ids


def _get_user_name(conn, user_id: int) -> str:
    """Возвращает login_display пользователя."""
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT login_display FROM {PM_USER_DB}.user WHERE id = %s",
            (user_id,)
        )
        row = cur.fetchone()
        return (row["login_display"] or "").strip() if row else f"user_{user_id}"
    except Exception:
        return f"user_{user_id}"


def _build_plain_messages(conn, thread_id: int, org_ids: set) -> tuple:
    """
    Строит текст диалога и определяет имена участников.
    Возвращает (plain_messages, visitor_name, operator_name, visitor_id, page_url).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT author_user_id, text, created_at
        FROM message
        WHERE thread_id = %s AND is_deleted = 0
        ORDER BY created_at
    """, (thread_id,))
    messages = cur.fetchall()

    if not messages:
        return "", "неизвестен", "неизвестен", "0", ""

    # Определяем участников
    participant_ids = {m["author_user_id"] for m in messages}
    org_participants = participant_ids & org_ids
    client_participants = participant_ids - org_ids

    # Имена
    operator_id  = next(iter(org_participants), None)
    visitor_id   = next(iter(client_participants), None)

    # Если оба — user (нет организаторов), author треда = клиент
    if not org_participants:
        cur.execute("SELECT author_user_id FROM thread WHERE id = %s", (thread_id,))
        row = cur.fetchone()
        visitor_id  = row["author_user_id"] if row else None
        operator_id = next((uid for uid in participant_ids if uid != visitor_id), visitor_id)

    visitor_name  = _get_user_name(conn, visitor_id)  if visitor_id  else "неизвестен"
    operator_name = _get_user_name(conn, operator_id) if operator_id else "неизвестен"

    # Ищем ссылку на закупку в тексте первого сообщения
    page_url = ""
    for m in messages[:3]:
        text = m["text"] or ""
        import re
        urls = re.findall(r'https?://[^\s\)\]]+', text)
        if urls:
            page_url = urls[0]
            break

    # Строим plain_messages
    lines = []
    for m in messages:
        uid = m["author_user_id"]
        name = visitor_name if uid == visitor_id else operator_name
        text = (m["text"] or "").strip()
        if text:
            lines.append(f"{name}: {text}")

    return (
        "\n".join(lines),
        visitor_name,
        operator_name,
        str(visitor_id or 0),
        page_url,
    )


# ---------------------------------------------------------------------------
# Основная функция: получить завершённые диалоги
# ---------------------------------------------------------------------------

def fetch_finished_dialogs(since: Optional[datetime] = None) -> list:
    """
    Возвращает список Dialog для тредов, где нет активности INACTIVITY_HOURS часов.

    since: не возвращать диалоги с last_msg_at <= since (для избежания дублей).
           Если None — берём всё за последние 30 дней.
    """
    from connectors.base import Dialog

    if since is None:
        since = datetime.now() - timedelta(days=30)

    deadline = datetime.now() - timedelta(hours=INACTIVITY_HOURS)

    conn = _get_conn()
    org_ids = _load_org_ids(conn)

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                t.id            AS thread_id,
                t.author_user_id,
                MAX(m.created_at) AS last_msg_at,
                COUNT(m.id)     AS msg_count
            FROM thread t
            JOIN message m ON m.thread_id = t.id AND m.is_deleted = 0
            GROUP BY t.id, t.author_user_id
            HAVING last_msg_at <= %s
               AND last_msg_at > %s
            ORDER BY last_msg_at ASC
        """, (deadline, since))

        rows = cur.fetchall()
        logger.info(f"[site_pm] Найдено завершённых тредов: {len(rows)}")

        dialogs = []
        for row in rows:
            thread_id = row["thread_id"]
            try:
                plain, visitor_name, operator_name, visitor_id, page_url = \
                    _build_plain_messages(conn, thread_id, org_ids)

                if not plain.strip():
                    continue

                # Считаем кол-во диалогов клиента
                cur.execute("""
                    SELECT COUNT(DISTINCT t2.id)
                    FROM thread t2
                    JOIN thread_user tu ON tu.thread_id = t2.id
                    WHERE tu.user_id = %s AND tu.is_deleted = 0
                """, (row["author_user_id"],))
                chats_count = (cur.fetchone() or {}).get("COUNT(DISTINCT t2.id)", 0)

                d = Dialog(
                    source        = "site_pm",
                    dialog_id     = str(thread_id),
                    chat_id       = thread_id,
                    finished_at   = row["last_msg_at"].isoformat(),
                    visitor_name  = visitor_name,
                    visitor_id    = visitor_id,
                    chats_count   = int(chats_count or 0),
                    operator_name = operator_name,
                    page_url      = page_url,
                    plain_messages = plain,
                    raw_json      = json.dumps({
                        "thread_id":   thread_id,
                        "last_msg_at": row["last_msg_at"].isoformat(),
                        "msg_count":   row["msg_count"],
                    }, ensure_ascii=False),
                )
                dialogs.append(d)
            except Exception as e:
                logger.error(f"[site_pm] Ошибка обработки thread_id={thread_id}: {e}")

        return dialogs

    finally:
        conn.close()
