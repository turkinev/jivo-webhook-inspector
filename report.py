"""
AI-отчёт по диалогам за день или неделю.
Собирает агрегаты из jivo_chat_analysis, передаёт в AI, отправляет в Telegram.

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

sys.path.insert(0, str(Path(__file__).parent))
from ai_processor import call_ai, CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ---------------------------------------------------------------------------
# Промпт для отчёта
# ---------------------------------------------------------------------------

REPORT_PROMPT = """\
Ты — аналитик службы поддержки интернет-магазина.
На основе статистики за период сформируй структурированный отчёт с выводами и рекомендациями.

Структура отчёта (используй Telegram Markdown: *жирный*, _курсив_, эмодзи):

1. 📋 Краткое резюме — 2-3 предложения, самое важное за период
2. 🔴 Проблемные зоны — что выросло, что критично, аномалии
   - Рост категории >20% — обязательно предположи причину на основе формулировок проблем
   - Укажи конкретные паттерны из sample_problems
3. 👨‍💼 Качество операторов — средняя оценка, динамика, кто требует внимания
4. ⚠️ Риски — клиенты с высоким churn и эскалации, процитируй 2-3 конкретных случая
5. 💡 Рекомендации — 3-5 конкретных действий на следующий период

Правила:
- Пиши конкретно, без воды и общих фраз
- Используй дельты: ↑ рост, ↓ падение, → без изменений
- Если total < 10 — предупреди что выборка маленькая и выводы ориентировочные
- Не пересказывай цифры — интерпретируй их

Данные за период:
{stats_json}
"""


# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def ch_query(sql: str) -> list:
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


# ---------------------------------------------------------------------------
# Сбор статистики
# ---------------------------------------------------------------------------

def period_filters(period: str):
    """Возвращает SQL-выражения границ текущего и предыдущего периода."""
    if period == "week":
        return (
            "toMonday(today())",
            "today() + 1",
            "toMonday(today()) - 7",
            "toMonday(today())",
        )
    # day
    return "today()", "today() + 1", "today() - 1", "today()"


def collect_stats(period: str) -> dict:
    cf, ct, pf, pt = period_filters(period)

    # -- Общие счётчики -------------------------------------------------
    rows = ch_query(f"""
        SELECT
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct})  AS cur_total,
            countIf(toDate(analyzed_at) >= {pf} AND toDate(analyzed_at) < {pt})  AS prev_total,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND user_emotion = 'Негатив')     AS cur_neg,
            countIf(toDate(analyzed_at) >= {pf} AND toDate(analyzed_at) < {pt} AND user_emotion = 'Негатив')     AS prev_neg,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND user_emotion = 'Нейтральный') AS cur_neu,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND user_emotion = 'Позитив')     AS cur_pos,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND resolution_status = 'Решено')    AS cur_resolved,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND resolution_status = 'Не решено') AS cur_unresolved,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND resolution_status = 'Частично')  AS cur_partial,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND resolution_status = 'Эскалация') AS cur_escalation,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND needs_escalation = 1)            AS cur_needs_esc,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct} AND churn_risk_score >= 0.8)         AS cur_high_churn,
            round(avgIf(agent_quality_score, toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct}), 1) AS cur_quality_avg,
            round(avgIf(agent_quality_score, toDate(analyzed_at) >= {pf} AND toDate(analyzed_at) < {pt}), 1) AS prev_quality_avg
        FROM jivo_chat_analysis
        FORMAT JSONEachRow
    """)
    t = rows[0] if rows else {}

    # -- Топ категорий с примерами проблем ------------------------------
    categories = ch_query(f"""
        SELECT
            category,
            countIf(toDate(analyzed_at) >= {cf} AND toDate(analyzed_at) < {ct})  AS cur_cnt,
            countIf(toDate(analyzed_at) >= {pf} AND toDate(analyzed_at) < {pt})  AS prev_cnt,
            groupArrayIf(5)(
                user_problem_summary,
                toDate(analyzed_at) >= {cf}
                    AND toDate(analyzed_at) < {ct}
                    AND user_problem_summary != ''
            ) AS sample_problems
        FROM jivo_chat_analysis
        WHERE category != 'Не определено'
        GROUP BY category
        HAVING cur_cnt > 0
        ORDER BY cur_cnt DESC
        LIMIT 8
        FORMAT JSONEachRow
    """)

    # -- Бизнес-сигналы -------------------------------------------------
    signals = ch_query(f"""
        SELECT
            business_signal,
            count() AS cnt
        FROM jivo_chat_analysis
        WHERE toDate(analyzed_at) >= {cf}
          AND toDate(analyzed_at) < {ct}
          AND business_signal != 'Нет сигнала'
        GROUP BY business_signal
        ORDER BY cnt DESC
        FORMAT JSONEachRow
    """)

    # -- Эскалации ------------------------------------------------------
    escalations = ch_query(f"""
        SELECT user_problem_summary
        FROM jivo_chat_analysis
        WHERE toDate(analyzed_at) >= {cf}
          AND toDate(analyzed_at) < {ct}
          AND needs_escalation = 1
          AND user_problem_summary != ''
        LIMIT 10
        FORMAT JSONEachRow
    """)

    # -- Высокий churn --------------------------------------------------
    high_churn = ch_query(f"""
        SELECT user_problem_summary
        FROM jivo_chat_analysis
        WHERE toDate(analyzed_at) >= {cf}
          AND toDate(analyzed_at) < {ct}
          AND churn_risk_score >= 0.8
          AND user_problem_summary != ''
        LIMIT 10
        FORMAT JSONEachRow
    """)

    # -- Операторы с низкой оценкой (join с jivo_chat_dialogs) ----------
    bad_agents = ch_query(f"""
        SELECT
            d.operator_name,
            round(avg(a.agent_quality_score), 1) AS avg_score,
            count() AS cnt,
            groupArray(3)(a.agent_quality_comment) AS comments
        FROM jivo_chat_analysis a
        JOIN jivo_chat_dialogs d ON a.chat_id = d.chat_id
        WHERE toDate(a.analyzed_at) >= {cf}
          AND toDate(a.analyzed_at) < {ct}
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
        "total":               int(t.get("cur_total", 0)),
        "prev_total":          int(t.get("prev_total", 0)),
        "emotions": {
            "Негатив":     int(t.get("cur_neg", 0)),
            "Нейтральный": int(t.get("cur_neu", 0)),
            "Позитив":     int(t.get("cur_pos", 0)),
        },
        "prev_neg":            int(t.get("prev_neg", 0)),
        "resolution": {
            "Решено":    int(t.get("cur_resolved", 0)),
            "Не решено": int(t.get("cur_unresolved", 0)),
            "Частично":  int(t.get("cur_partial", 0)),
            "Эскалация": int(t.get("cur_escalation", 0)),
        },
        "needs_escalation":    int(t.get("cur_needs_esc", 0)),
        "high_churn_count":    int(t.get("cur_high_churn", 0)),
        "agent_quality_avg":   float(t.get("cur_quality_avg", 0)),
        "prev_agent_quality_avg": float(t.get("prev_quality_avg", 0)),
        "categories":          categories,
        "business_signals":    signals,
        "escalation_problems": [r["user_problem_summary"] for r in escalations],
        "high_churn_problems": [r["user_problem_summary"] for r in high_churn],
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", choices=["day", "week"], default="day",
                        help="day = сегодня vs вчера, week = эта неделя vs прошлая")
    parser.add_argument("--dry-run", action="store_true",
                        help="не отправлять в Telegram, вывести в консоль")
    args = parser.parse_args()

    print(f"Собираем статистику [{args.period}]...")
    stats = collect_stats(args.period)
    print(f"Диалогов: {stats['total']} (предыдущий период: {stats['prev_total']})")

    if stats["total"] == 0:
        print("Нет данных за период. Отчёт не формируется.")
        return

    stats_json = json.dumps(stats, ensure_ascii=False, indent=2)
    prompt = REPORT_PROMPT.format(stats_json=stats_json)

    if args.dry_run:
        print("\n=== ПРОМПТ ДЛЯ AI ===")
        print(prompt[:1000], "...[сокращено]")

    print("Отправляем в AI...")
    report_text = call_ai(prompt)

    if not report_text:
        print("[error] AI не ответил")
        return

    period_label = {"day": "день", "week": "неделю"}[args.period]
    full_text = f"📊 *Отчёт за {period_label} — {stats['date']}*\n\n{report_text}"

    if args.dry_run:
        print("\n=== ОТЧЁТ ===")
        print(full_text)
        return

    send_telegram(full_text)


if __name__ == "__main__":
    main()
