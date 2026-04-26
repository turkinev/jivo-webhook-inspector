"""
AI-отчёт по диалогам за день или неделю.
Собирает агрегаты из dialog_analysis, передаёт в AI, отправляет в Telegram.

Запуск:
    python3 report.py [--period day|week] [--dry-run]

--period day    отчёт за сегодня vs вчера (по умолчанию)
--period week   отчёт за текущую неделю vs прошлую
--dry-run       не отправлять в Telegram, вывести в консоль
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
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

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

sys.path.insert(0, str(Path(__file__).parent))
from ai_processor import call_ai, CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
REPORT_MAX_TOKENS   = int(os.getenv("REPORT_MAX_TOKENS", "4000"))
MM_REPORT_WEBHOOK   = os.getenv("MM_REPORT_WEBHOOK", "")
REPORT_AI_MODEL     = os.getenv("REPORT_AI_MODEL", "openai/gpt-5.4")

# ---------------------------------------------------------------------------
# Промпт для отчёта
# ---------------------------------------------------------------------------

REPORT_PROMPT = """\
Ты — аналитик поддержки. Сформируй отчёт строго на основе переданных данных.

Правила:
- Только факты из данных. Никаких предположений, «вероятно», «возможно».
- Никакой воды. Каждое предложение = сигнал или цифра.
- Сравнение только с медианой за {median_days} дней: ↑/↓ к медиане, аномалию помечай 🔺 или 🔻 — определяй сам.
- Цитируй формулировки из all_problems дословно, без редактуры.

Отделы и зоны ответственности:
- Логистика: ПВЗ, движение товара, приём от поставщика/ТК, доставка, задержки, пересорты при сортировке и доставке, бой, возвраты, пристрой
- IT: технические ошибки, сбои сайта
- Модераторы: качество работы организаторов, качество товара, конфликты между участниками, правила сайта
- Служба поддержки: качество коммуникаций, удовлетворённость пользователей
- Продукт: удобство сайта, дизайн, UX, пользовательский путь, запросы на новые функции
- Категорийный отдел: ассортимент, поставщики, бренды, категории товаров
- Маркетинг: рассылки, уведомления, рекламные активности
- Не определено: всё остальное

Структура отчёта (Markdown **жирный**, эмодзи):

**📊 Сводка**
N диалогов (↑/↓ к медиане {median_days}д)
Решено X% | Не решено X% | Эскалация X
Качество операторов: X/100
Негатив: X%

**📂 Все категории** (в порядке убывания, включая «Не определено»):
Формат каждой строки: Название — N (↑/↓ к медиане) [🔺 если аномалия]

**🏢 По отделам** (только у кого есть обращения, в порядке убывания):
Для каждого отдела:
  **Название отдела — N обращений**
  Ключевые сигналы: 1-2 предложения по сути проблем
  Все обращения:
  • [дословно из all_problems]
  • ...

**⚠️ Требуют реакции** (только если есть):
- Эскалации: процитируй каждую
- Риск оттока ≥ 0.8: процитируй каждую
- Операторы с низкой оценкой: имя + кол-во диалогов + комментарий

**💡 Системные сигналы**
Паттерны повторяющиеся в 3+ обращениях. Формат: «[проблема] — N раз → отдел»

Данные:
{stats_json}
"""


# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def ch_query(sql: str) -> list:
    # Параметры аутентификации в URL, тело запроса — SQL (POST избегает ограничений длины URL)
    params = urllib.parse.urlencode({
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"CH HTTP {e.code}: {body[:500]}") from None
    rows = []
    for line in resp.read().decode().strip().splitlines():
        if line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


# ---------------------------------------------------------------------------
# Сбор статистики
# ---------------------------------------------------------------------------

def period_filters(period: str):
    """Возвращает SQL-выражения границ периода (от, до)."""
    if period == "week":
        return "toMonday(today())", "today() + 1"
    if period == "yesterday":
        return "today() - 1", "today()"
    # day = сегодня
    return "today()", "today() + 1"


MEDIAN_DAYS = int(os.getenv("REPORT_MEDIAN_DAYS", "30"))


def collect_stats(period: str) -> dict:
    cf, ct = period_filters(period)

    ts_subquery = """(
        SELECT chat_id, max(received_at) AS ts
        FROM raw_dialogs
        WHERE event_name = 'chat_finished'
        GROUP BY chat_id
    )"""

    # -- Общие счётчики -------------------------------------------------
    rows = ch_query(f"""
        SELECT
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct})  AS cur_total,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.user_emotion = 'Негатив')     AS cur_neg,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.user_emotion = 'Нейтральный') AS cur_neu,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.user_emotion = 'Позитив')     AS cur_pos,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.resolution_status = 'Решено')    AS cur_resolved,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.resolution_status = 'Не решено') AS cur_unresolved,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.resolution_status = 'Частично')  AS cur_partial,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.resolution_status = 'Эскалация') AS cur_escalation,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.needs_escalation = 1)            AS cur_needs_esc,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND ifNull(a.churn_risk_score, 0) >= 0.8) AS cur_high_churn,
            round(avgIf(a.agent_quality_score, toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct}), 1) AS cur_quality_avg
        FROM dialog_analysis a
        JOIN {ts_subquery} r ON a.chat_id = r.chat_id
        FORMAT JSONEachRow
    """)
    t = rows[0] if rows else {}

    # -- Медиана общего объёма за N дней --------------------------------
    median_rows = ch_query(f"""
        SELECT round(avg(daily_cnt), 1) AS median_total
        FROM (
            SELECT toDate(r.ts) AS day, count() AS daily_cnt
            FROM dialog_analysis a
            JOIN {ts_subquery} r ON a.chat_id = r.chat_id
            WHERE toDate(r.ts) >= today() - {MEDIAN_DAYS}
              AND toDate(r.ts) < {cf}
            GROUP BY day
        )
        FORMAT JSONEachRow
    """)
    median_total = float((median_rows[0] if median_rows else {}).get("median_total") or 0)

    # -- Медиана по категориям за N дней --------------------------------
    median_cat_rows = ch_query(f"""
        SELECT category, round(avg(daily_cnt), 1) AS median_daily
        FROM (
            SELECT toDate(r.ts) AS day, a.category, count() AS daily_cnt
            FROM dialog_analysis a
            JOIN {ts_subquery} r ON a.chat_id = r.chat_id
            WHERE toDate(r.ts) >= today() - {MEDIAN_DAYS}
              AND toDate(r.ts) < {cf}
            GROUP BY day, a.category
        )
        GROUP BY category
        FORMAT JSONEachRow
    """)
    median_by_cat = {r["category"]: float(r["median_daily"]) for r in median_cat_rows}

    # -- Все категории с полным списком проблем -------------------------
    categories = ch_query(f"""
        SELECT
            a.category AS category,
            countIf(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct}) AS cur_cnt,
            arrayFilter(x -> x != '', groupArray(
                if(toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct} AND a.user_problem_summary != '',
                   concat('#', toString(a.chat_id), ': ', a.user_problem_summary), '')
            )) AS all_problems
        FROM dialog_analysis a
        JOIN {ts_subquery} r ON a.chat_id = r.chat_id
        GROUP BY a.category
        HAVING cur_cnt > 0
        ORDER BY cur_cnt DESC
        FORMAT JSONEachRow
    """)
    # Добавляем медиану к каждой категории
    for cat in categories:
        cat["median_daily"] = median_by_cat.get(cat["category"], 0)

    # -- Эскалации ------------------------------------------------------
    escalations = ch_query(f"""
        SELECT concat('#', toString(a.chat_id), ': ', a.user_problem_summary) AS problem
        FROM dialog_analysis a
        JOIN {ts_subquery} r ON a.chat_id = r.chat_id
        WHERE toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct}
          AND a.needs_escalation = 1 AND a.user_problem_summary != ''
        FORMAT JSONEachRow
    """)

    # -- Высокий churn --------------------------------------------------
    high_churn = ch_query(f"""
        SELECT concat('#', toString(a.chat_id), ': ', a.user_problem_summary) AS problem
        FROM dialog_analysis a
        JOIN {ts_subquery} r ON a.chat_id = r.chat_id
        WHERE toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct}
          AND ifNull(a.churn_risk_score, 0) >= 0.8
          AND a.user_problem_summary != ''
        FORMAT JSONEachRow
    """)

    # -- Операторы с низкой оценкой -------------------------------------
    bad_agents = ch_query(f"""
        SELECT
            d.operator_name AS operator_name,
            round(avg(a.agent_quality_score), 1) AS avg_score,
            count() AS cnt,
            groupArray(3)(a.agent_quality_comment) AS comments
        FROM dialog_analysis a
        JOIN {ts_subquery} r ON a.chat_id = r.chat_id
        JOIN dialogs d ON a.chat_id = d.chat_id
        WHERE toDate(r.ts) >= {cf} AND toDate(r.ts) < {ct}
          AND a.agent_quality_label IN ('Плохо', 'Удовлетворительно')
          AND d.operator_name != ''
        GROUP BY d.operator_name
        HAVING cnt >= 2
        ORDER BY avg_score ASC
        LIMIT 5
        FORMAT JSONEachRow
    """)

    return {
        "period":              period,
        "date":                str(date.today()),
        "median_days":         MEDIAN_DAYS,
        "total":               int(t.get("cur_total", 0)),
        "median_total":        median_total,
        "emotions": {
            "Негатив":     int(t.get("cur_neg", 0)),
            "Нейтральный": int(t.get("cur_neu", 0)),
            "Позитив":     int(t.get("cur_pos", 0)),
        },
        "resolution": {
            "Решено":    int(t.get("cur_resolved", 0)),
            "Не решено": int(t.get("cur_unresolved", 0)),
            "Частично":  int(t.get("cur_partial", 0)),
            "Эскалация": int(t.get("cur_escalation", 0)),
        },
        "needs_escalation":    int(t.get("cur_needs_esc", 0)),
        "high_churn_count":    int(t.get("cur_high_churn", 0)),
        "agent_quality_avg":   float(t.get("cur_quality_avg") or 0),
        "categories":          categories,
        "escalation_problems": [r["problem"] for r in escalations],
        "high_churn_problems": [r["problem"] for r in high_churn],
        "low_quality_operators": bad_agents,
    }


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[warn] TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы в .env")
        return
    # Telegram ограничивает сообщение 4096 символами
    if len(text) > 4000:
        text = text[:3997] + "..."
    body = json.dumps({
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "Markdown",
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        result = json.loads(resp.read().decode())
        if not result.get("ok"):
            print(f"[error] Telegram: {result}")
        else:
            print("[ok] Отчёт отправлен в Telegram")
    except Exception as e:
        print(f"[error] Telegram: {e}")


MM_CHUNK_SIZE = int(os.getenv("MM_CHUNK_SIZE", "14000"))


def _mm_chunks(text: str) -> list[str]:
    """Разбивает текст на части до MM_CHUNK_SIZE символов по границам строк."""
    if len(text) <= MM_CHUNK_SIZE:
        return [text]
    chunks, current = [], []
    size = 0
    for line in text.splitlines(keepends=True):
        if size + len(line) > MM_CHUNK_SIZE and current:
            chunks.append("".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


def _mm_post(text: str):
    body = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    req  = urllib.request.Request(
        MM_REPORT_WEBHOOK,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=15)


def send_mattermost(text: str):
    """Отправляет отчёт в Mattermost, разбивая на части если текст длиннее MM_CHUNK_SIZE."""
    if not MM_REPORT_WEBHOOK:
        print("[warn] MM_REPORT_WEBHOOK не задан в .env")
        return
    chunks = _mm_chunks(text)
    try:
        for i, chunk in enumerate(chunks, 1):
            _mm_post(chunk)
            if len(chunks) > 1:
                print(f"[ok] Mattermost часть {i}/{len(chunks)}")
        print(f"[ok] Отчёт отправлен в Mattermost ({len(chunks)} сообщ., {len(text)} симв.)")
    except Exception as e:
        print(f"[error] Mattermost: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", choices=["day", "yesterday", "week"], default="yesterday",
                        help="yesterday = вчера vs позавчера (по умолчанию), day = сегодня vs вчера, week = эта неделя vs прошлая")
    parser.add_argument("--dry-run", action="store_true",
                        help="не отправлять в Telegram, вывести в консоль")
    args = parser.parse_args()

    print(f"Собираем статистику [{args.period}]...")
    stats = collect_stats(args.period)
    print(f"Диалогов: {stats['total']} (медиана {stats['median_days']}д: {stats['median_total']})")

    if stats["total"] == 0:
        print("Нет данных за период. Отчёт не формируется.")
        return

    stats_json = json.dumps(stats, ensure_ascii=False, indent=2)
    prompt = REPORT_PROMPT.format(stats_json=stats_json, median_days=MEDIAN_DAYS)

    if args.dry_run:
        print("\n=== ПРОМПТ ДЛЯ AI ===")
        print(prompt[:1000], "...[сокращено]")

    print(f"Размер промпта: {len(prompt)} символов")
    print("Отправляем в AI...")
    report_text = call_ai(prompt, max_tokens=REPORT_MAX_TOKENS, model=REPORT_AI_MODEL)

    if not report_text:
        print("[error] AI не ответил")
        return

    period_label = {"day": "день (сегодня)", "yesterday": "день (вчера)", "week": "неделю"}[args.period]
    full_text = f"📊 *Отчёт за {period_label} — {stats['date']}*\n\n{report_text}"

    if args.dry_run:
        print("\n=== ОТЧЁТ ===")
        print(full_text)
        return

    send_telegram(full_text)
    send_mattermost(full_text)


if __name__ == "__main__":
    main()
