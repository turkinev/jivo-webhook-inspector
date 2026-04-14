"""
AI-анализ завершённых чатов JivoChat.
Запускается в фоне после сохранения хука в ClickHouse.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Конфиг — из переменных окружения
# ---------------------------------------------------------------------------

AI_PROXY_URL        = os.getenv("AI_PROXY_URL", "https://naitislova.ru/_ai/proxy")
AI_API_KEY          = os.getenv("AI_API_KEY", "")
AI_MODEL            = os.getenv("AI_MODEL", "openai/gpt-4o-mini")
AI_TEMPERATURE      = float(os.getenv("AI_TEMPERATURE", "0.4"))
AI_MAX_TOKENS       = int(os.getenv("AI_MAX_TOKENS", "2000"))
AI_MAX_RETRIES      = int(os.getenv("AI_MAX_RETRIES", "3"))
AI_RETRY_DELAYS     = [2, 5, 15]

CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

ANALYSIS_PROMPT = """\
Ты — аналитик службы поддержки интернет-магазина. Проанализируй диалог чата \
и верни строго JSON без markdown-обёртки.

Выбери РОВНО одну категорию (category) и РОВНО одну подкатегорию (subcategory) \
из списка ниже. Не придумывай новые значения. \
Если не уверен — выбирай более общую подкатегорию.

Допустимые значения category и subcategory (используй точные названия без номеров):

Заказ и доставка
   Статус заказа / задержка
   Заказ не найден / потерян
   Статус не обновился
   Объединение / формирование посылки
   Возврат товара (логистика)
ПВЗ и выдача
   График / адрес ПВЗ
   Заказ не выдается
   Проблема при приеме товара
   Не сканируется / нет стикера
   Ручные операции / обход процесса
   Стать раздающим
Оплата и финансы
   Возврат денег
   Непонятные списания / долг
   Платное хранение
   Оргсбор — размер и расчёт
Технические проблемы
   Ошибка / сбой сайта
   Некорректное отображение
   Поиск / каталог
   Мобильное приложение
Аккаунт и профиль
   Нет доступа / ошибка входа
   Смена телефона / почты
   Блокировка / ограничения
   Управление рассылками
Качество товара
   Брак / дефект
   Несоответствие описанию или фото
   Пересорт / пришло не то
   Повреждение при доставке
   Нарушения — маркировка, сроки годности
Закупки и организаторы
   Вопрос по закупке
   Статус закупки / оплаты
   Жалоба на организатора
Пристрой
   Как работает пристрой
   Проблемы / передача пристроя
Пожелания и инсайты
   Запрос нового бренда / поставщика
   Запрос открытия ПВЗ в регионе
   Запрос новой функции платформы
   Коммерческое предложение / партнёрство
   Сравнение с конкурентами (WB, Ozon)
Благодарность
   Благодарность
Не определено
   Служебное / внутреннее

Пример: если заказ задержался — category: "Заказ и доставка", subcategory: "Статус заказа / задержка"

Остальные фиксированные списки:

user_emotion: позитив | нейтральный | раздражение | злость

resolution_status: решено | не_решено | частично | эскалация

business_signal: проблема_доставки | брак_товара | проблема_оплаты | \
ux_проблема | риск_оттока | позитивный_отзыв | проблема_процесса | другое

agent_quality_label: отлично | хорошо | удовлетворительно | плохо

Тип обращения (contact_reason) — выбери РОВНО ОДИН наиболее подходящий:

Жалоба          — недовольство, претензия, раздражение («устал ждать», «это не нормально», «мне надоело», «вы не решаете проблему»)
Консультация    — хочет понять процесс или что делать («как мне», «подскажите», «как это работает», «что делать если»)
Проблема        — что-то не работает, без сильного негатива («не работает», «ошибка», «не получается», «не проходит»)
Статус          — узнать текущее состояние заказа/процесса («где заказ», «когда будет», «какой статус», «есть ли информация»)
Запрос действия — просит сделать что-то конкретное («верните деньги», «отмените заказ», «исправьте», «перенесите»)
Уточнение       — короткий доп. вопрос после ответа («правильно понимаю?», «то есть нужно...?», «это вот эта кнопка?»)
Благодарность   — позитивное завершение («спасибо», «понял, благодарю», «всё ок»)
Другое          — если ни один вариант не подходит

Формат ответа — строго JSON, без пояснений:
{{
  "contact_reason": "одно из: Жалоба | Консультация | Проблема | Статус | Запрос действия | Уточнение | Благодарность | Другое",
  "category": "Название категории — точно из списка выше",
  "subcategory": "Название подкатегории — точно из списка выше",
  "user_problem_summary": "краткое описание проблемы 1-2 предложения",
  "user_emotion": "...",
  "churn_risk_score": 0.0,
  "resolution_status": "...",
  "needs_escalation": 0,
  "agent_quality_score": 75,
  "agent_quality_label": "...",
  "agent_quality_comment": "что сделал хорошо/плохо, 1 предложение",
  "business_signal": "...",
  "root_cause_guess": "предположение о корневой причине, кратко",
  "insight_comment": "полезный инсайт для продукта/процесса, кратко"
}}

Диалог чата:
---
Посетитель: {visitor_name}
Количество обращений: {chats_count}
Страница: {page_url}
Оператор: {operator_name}

{plain_messages}
---
"""


def build_prompt(payload: dict) -> str:
    visitor  = payload.get("visitor") or {}
    agents   = payload.get("agents") or []
    agent    = agents[0] if agents else {}
    page     = payload.get("page") or {}

    return ANALYSIS_PROMPT.format(
        visitor_name   = visitor.get("name") or "неизвестен",
        chats_count    = visitor.get("chats_count") or 0,
        page_url       = page.get("url") or "",
        operator_name  = agent.get("name") or "неизвестен",
        plain_messages = payload.get("plain_messages") or "",
    )


# ---------------------------------------------------------------------------
# AI call
# ---------------------------------------------------------------------------

def call_ai(prompt: str) -> Optional[str]:
    """Вызывает AI через прокси, возвращает текст ответа или None при ошибке."""
    headers_bytes = (
        f"Authorization: Bearer {AI_API_KEY}\r\n"
        f"Content-Type: application/json\r\n"
    ).encode()

    body = json.dumps({
        "model":       AI_MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "temperature": AI_TEMPERATURE,
        "max_tokens":  AI_MAX_TOKENS,
        "stream":      False,
    }, ensure_ascii=False).encode("utf-8")

    RETRYABLE = {429, 500, 502, 503, 504}

    for attempt in range(AI_MAX_RETRIES):
        try:
            req = urllib.request.Request(
                AI_PROXY_URL,
                data=body,
                method="POST",
                headers={
                    "Authorization": f"Bearer {AI_API_KEY}",
                    "Content-Type":  "application/json",
                },
            )
            resp = urllib.request.urlopen(req, timeout=60)
            result = json.loads(resp.read().decode())
            return result["choices"][0]["message"]["content"]

        except urllib.error.HTTPError as e:
            code = e.code
            if code in RETRYABLE and attempt < AI_MAX_RETRIES - 1:
                time.sleep(AI_RETRY_DELAYS[attempt])
                continue
            logger.error(f"[AI] HTTP {code}: {e.read().decode()[:200]}")
            return None

        except Exception as e:
            if attempt < AI_MAX_RETRIES - 1:
                time.sleep(AI_RETRY_DELAYS[attempt])
                continue
            logger.error(f"[AI] Ошибка: {e}")
            return None

    return None


# ---------------------------------------------------------------------------
# Parse AI response
# ---------------------------------------------------------------------------

def parse_response(ai_text: str) -> Optional[dict]:
    """Парсит JSON из ответа модели, снимает markdown-обёртку если есть."""
    text = ai_text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) >= 2 else text
        text = text.removeprefix("json").strip()

    try:
        data = json.loads(text)
        return {
            "contact_reason":        str(data.get("contact_reason") or "другое"),
            "category":              str(data.get("category") or "другое"),
            "subcategory":           str(data.get("subcategory") or ""),
            "user_problem_summary":  str(data.get("user_problem_summary") or ""),
            "user_emotion":          str(data.get("user_emotion") or "нейтральный"),
            "churn_risk_score":      float(data.get("churn_risk_score") or 0.0),
            "resolution_status":     str(data.get("resolution_status") or "не_решено"),
            "needs_escalation":      int(bool(data.get("needs_escalation"))),
            "agent_quality_score":   int(data.get("agent_quality_score") or 0),
            "agent_quality_label":   str(data.get("agent_quality_label") or ""),
            "agent_quality_comment": str(data.get("agent_quality_comment") or ""),
            "business_signal":       str(data.get("business_signal") or "другое"),
            "root_cause_guess":      str(data.get("root_cause_guess") or ""),
            "insight_comment":       str(data.get("insight_comment") or ""),
        }
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"[AI] Не удалось распарсить ответ: {e}\n{text[:300]}")
        return None


# ---------------------------------------------------------------------------
# ClickHouse insert
# ---------------------------------------------------------------------------

def ch_request(query: str, data: bytes = None) -> str:
    params = urllib.parse.urlencode({
        "query":    query,
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    resp = urllib.request.urlopen(req, timeout=15)
    return resp.read().decode()


def insert_analysis(chat_id: int, parsed: dict, raw_llm_json: str):
    row = json.dumps({
        "chat_id":               chat_id,
        "contact_reason":        parsed["contact_reason"],
        "category":              parsed["category"],
        "subcategory":           parsed["subcategory"],
        "user_problem_summary":  parsed["user_problem_summary"],
        "user_emotion":          parsed["user_emotion"],
        "churn_risk_score":      parsed["churn_risk_score"],
        "resolution_status":     parsed["resolution_status"],
        "needs_escalation":      parsed["needs_escalation"],
        "agent_quality_score":   parsed["agent_quality_score"],
        "agent_quality_label":   parsed["agent_quality_label"],
        "agent_quality_comment": parsed["agent_quality_comment"],
        "business_signal":       parsed["business_signal"],
        "root_cause_guess":      parsed["root_cause_guess"],
        "insight_comment":       parsed["insight_comment"],
        "model_name":            AI_MODEL,
        "raw_llm_json":          raw_llm_json,
    }, ensure_ascii=False)

    ch_request(
        "INSERT INTO jivo_chat_analysis FORMAT JSONEachRow",
        data=row.encode("utf-8"),
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_and_save(payload: dict):
    """
    Полный цикл: промпт → AI → парсинг → CH.
    Вызывается из BackgroundTasks FastAPI.
    """
    chat_id = int(payload.get("chat_id") or 0)
    logger.info(f"[AI] start chat_id={chat_id} model={AI_MODEL}")

    prompt = build_prompt(payload)
    ai_text = call_ai(prompt)

    if not ai_text:
        logger.error(f"[AI] no response for chat_id={chat_id}")
        return

    parsed = parse_response(ai_text)
    if not parsed:
        logger.error(f"[AI] parse failed for chat_id={chat_id}")
        return

    try:
        insert_analysis(chat_id, parsed, ai_text)
        logger.info(
            f"[AI] saved chat_id={chat_id} "
            f"category={parsed['category']} "
            f"emotion={parsed['user_emotion']} "
            f"resolution={parsed['resolution_status']} "
            f"quality={parsed['agent_quality_score']}"
        )
    except Exception as e:
        logger.error(f"[AI] CH insert failed for chat_id={chat_id}: {e}")
