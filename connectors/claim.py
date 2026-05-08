"""
Коннектор: обращения через форму сайта (MySQL msg.claim + msg.post).

Источник данных:
  host: 10.1.100.61  db: msg  user: dev
  claim: id, post_id, type, status, closed_at, url
  post:  id, author_user_id, text (mediumtext) — текст обращения

Настройки (env):
  PM_DB_HOST / PM_DB_PORT / PM_DB_USER / PM_DB_PASSWORD / PM_DB_NAME
    — те же что у site_pm (база msg на том же хосте)
  CLAIM_POST_TEXT_COLUMN
    — имя колонки с текстом в таблице post (по умолчанию 'text')
  PM_USER_DB
    — имя базы с таблицей user (по умолчанию 'user')

Логика: забираем все записи со status='closed' и closed_at > cursor.
Курсор хранится в poller_cursor (source='claim').
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Переиспользуем те же настройки что у site_pm — база та же
CLAIM_DB_HOST     = os.getenv("PM_DB_HOST",     "10.1.100.61")
CLAIM_DB_PORT     = int(os.getenv("PM_DB_PORT", "3306"))
CLAIM_DB_USER     = os.getenv("PM_DB_USER",     "dev")
CLAIM_DB_PASSWORD = os.getenv("PM_DB_PASSWORD", "dev")
CLAIM_DB_NAME     = os.getenv("PM_DB_NAME",     "msg")
CLAIM_USER_DB     = os.getenv("PM_USER_DB",     "user")

# Колонка с текстом в таблице post (по умолчанию 'text' — подтверждено через DESCRIBE)
POST_TEXT_COLUMN = os.getenv("CLAIM_POST_TEXT_COLUMN", "text")

CLAIM_TYPE_RU: dict[str, str] = {
    "review":        "Отзыв",
    "qa":            "Вопрос-ответ",
    "message":       "Сообщение",
    "footer":        "Обращение",
    "item_price":    "Цена товара",
    "support":       "Поддержка",
    "rating_org":    "Рейтинг организатора",
    "refund_order":  "Возврат заказа",
    "delete_user":   "Удаление аккаунта",
    "found_cheaper": "Нашёл дешевле",
}


def _get_conn():
    try:
        import pymysql
        return pymysql.connect(
            host=CLAIM_DB_HOST,
            port=CLAIM_DB_PORT,
            user=CLAIM_DB_USER,
            password=CLAIM_DB_PASSWORD,
            database=CLAIM_DB_NAME,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except ImportError:
        raise RuntimeError("pymysql не установлен: pip install pymysql")


def fetch_finished_dialogs(since: Optional[datetime] = None) -> list:
    """
    Возвращает список Dialog для закрытых обращений (claim.status='closed').

    since: нижняя граница по closed_at (из poller_cursor).
           Если None — берём за последние 30 дней.
    """
    from connectors.base import Dialog

    if since is None:
        since = datetime.now() - timedelta(days=30)

    conn = _get_conn()
    try:
        cur = conn.cursor()

        # Динамически подставляем имя колонки — pymysql не умеет параметризовать идентификаторы
        cur.execute(f"""
            SELECT
                c.id                  AS claim_id,
                c.type                AS claim_type,
                c.closed_at           AS closed_at,
                c.url                 AS url,
                p.{POST_TEXT_COLUMN}  AS post_text,
                p.author_user_id      AS author_user_id,
                u.login_display       AS author_name
            FROM claim c
            LEFT JOIN post p ON p.id = c.post_id
            LEFT JOIN {CLAIM_USER_DB}.user u ON u.id = p.author_user_id
            WHERE c.status = 'closed'
              AND c.closed_at > %s
            ORDER BY c.closed_at ASC
        """, (since,))

        rows = cur.fetchall()
        logger.info(f"[claim] Найдено закрытых обращений: {len(rows)}")

        dialogs = []
        for row in rows:
            claim_id     = row["claim_id"]
            claim_type   = row["claim_type"] or "message"
            type_ru      = CLAIM_TYPE_RU.get(claim_type, claim_type)
            post_text    = (row["post_text"] or "").strip()
            closed_at    = row["closed_at"]
            url          = row["url"] or ""
            author_id    = row.get("author_user_id") or 0
            author_name  = (row.get("author_name") or "").strip() or f"user_{author_id}"

            if not post_text:
                logger.debug(f"[claim] Пропускаем claim_id={claim_id} — пустой текст")
                continue

            # Формируем текст для AI: заголовок + сам текст
            lines = []
            if url:
                lines.append(f"[Страница: {url}]")
            lines.append(f"{author_name}: {post_text}")
            plain = "\n".join(lines)

            if isinstance(closed_at, datetime):
                finished = closed_at.isoformat()
            else:
                finished = str(closed_at)

            d = Dialog(
                source         = "claim",
                dialog_id      = str(claim_id),
                chat_id        = claim_id,
                finished_at    = finished,
                visitor_name   = author_name,
                visitor_id     = str(author_id),
                chats_count    = 0,
                operator_name  = "Модератор",
                page_url       = url,
                plain_messages = plain,
                widget_id      = claim_type,  # сырой тип для хранения в CH
                raw_json       = json.dumps({
                    "claim_id":    claim_id,
                    "type":        claim_type,
                    "type_ru":     type_ru,
                    "status":      "closed",
                    "closed_at":   finished,
                    "url":         url,
                    "author_id":   author_id,
                    "author_name": author_name,
                }, ensure_ascii=False),
            )
            dialogs.append(d)

        return dialogs

    finally:
        conn.close()
