# Dialog Analytics — Техническая документация

## Обзор системы

Система собирает завершённые диалоги поддержки из нескольких источников, анализирует их через AI и сохраняет результаты в ClickHouse. На основе накопленных данных формируются ежедневные/еженедельные отчёты и отслеживаются нарушения SLA.

---

## Архитектура

```
┌──────────────────────────────────────────────────────────┐
│                      Источники данных                     │
│                                                           │
│  JivoChat webhook  ──►  main.py (FastAPI, порт 62000)    │
│                                                           │
│  MySQL (ЛС сайта)  ──►  poller.py  (cron, каждые 15 мин)│
│                    └──►  connectors/site_pm.py            │
└──────────────────────────────┬───────────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   ai_processor.py    │
                    │   GPT-4o-mini через  │
                    │   AI-прокси          │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │          ClickHouse             │
              │  raw_dialogs                   │
              │  dialogs                       │
              │  dialog_analysis               │
              │  dialog_sla                    │
              │  poller_cursor                 │
              └──────────┬─────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
          ▼                             ▼
   report.py                    sla_checker.py
   (cron, Telegram)             (cron, каждые 30 мин)
```

---

## Статус компонентов

| Компонент | Статус | Примечание |
|---|---|---|
| `main.py` — webhook-сервер | ✅ Работает | systemd, порт 62000 |
| `ai_processor.py` — AI-анализ | ✅ Работает | Запускается из main.py и poller.py |
| `poller.py` — поллер ЛС | ✅ Работает | Запускается вручную, cron не настроен |
| `report.py` — отчёты | ✅ Работает | Запускается вручную, cron не настроен |
| `sla_checker.py` — SLA | ✅ Работает | Запускается вручную, cron не настроен |
| Cron-расписание | ❌ Не настроен | Требует `crontab -e` на сервере |

---

## Компоненты

### `main.py` — Webhook-сервер (FastAPI)

Принимает события от JivoChat через HTTP POST.

| Endpoint | Метод | Назначение |
|---|---|---|
| `/jivo/webhook` | POST | Приём событий JivoChat |
| `/jivo/logs` | GET | Последние 50 диалогов из CH |
| `/health` | GET | Статус сервиса + CH |

**Поток обработки:**
1. Получить JSON от JivoChat
2. Проверить `event_name == "chat_finished"`
3. Записать raw payload в `raw_dialogs`
4. Записать структурированные поля в `dialogs`
5. Запустить `analyze_and_save()` в фоне (BackgroundTasks)

Сервис отвечает `{"result": "ok"}` немедленно — AI работает асинхронно.

---

### `ai_processor.py` — AI-анализ диалогов

Вызывается из `main.py` (фоново) и из `poller.py`.

**Функции:**
- `build_prompt(payload)` — формирует промпт из данных диалога
- `call_ai(prompt, max_tokens)` — отправляет запрос в AI с retry (3 попытки, 429/5xx)
- `parse_response(ai_text)` — парсит JSON из ответа, снимает markdown-обёртку
- `insert_analysis(chat_id, parsed, raw_llm_json, source)` — вставляет в `dialog_analysis`
- `analyze_and_save(payload)` — полный цикл: промпт → AI → парсинг → CH

**AI-поля, которые заполняются:**

| Поле | Тип | Описание |
|---|---|---|
| `source_type` | string | ПВЗ / Сотрудник / Клиент |
| `contact_reason` | string | Жалоба / Консультация / Проблема / Статус / Запрос действия / Уточнение / Благодарность |
| `category` | string | Категория обращения (10 категорий) |
| `subcategory` | string | Подкатегория (40+ вариантов) |
| `user_problem_summary` | string | Краткое описание проблемы (1-2 предложения) |
| `user_emotion` | string | Позитив / Нейтральный / Негатив |
| `churn_risk_score` | float | Риск оттока 0.0–1.0 |
| `resolution_status` | string | Решено / Не решено / Частично / Эскалация |
| `needs_escalation` | int | 0 или 1 |
| `agent_quality_score` | int | 0–100 |
| `agent_quality_label` | string | Отлично / Хорошо / Удовлетворительно / Плохо |
| `agent_quality_comment` | string | Комментарий по качеству работы оператора |
| `business_signal` | string | Логистика / ПВЗ / Финансы / Продукт / Организаторы / Товар / Поддержка / Рост / Нет сигнала |
| `root_cause_guess` | string | Предположение о корневой причине |
| `insight_comment` | string | Инсайт для продукта или процессов |

---

### `poller.py` — Поллер DB-источников

Запускается по cron (каждые 15 минут). Забирает завершённые диалоги из всех DB-источников.

**Алгоритм:**
1. Загрузить курсор (`poller_cursor`) — последний обработанный timestamp
2. Получить `processed_ids` из `dialog_analysis` (защита от повторной обработки)
3. Вызвать `fetch_finished_dialogs(since)` у каждого источника
4. Отфильтровать уже обработанные диалоги
5. Для каждого нового диалога: `save_raw()` → `save_dialog()` → `analyze_and_save()`
6. Обновить курсор

**Параметры запуска:**
```bash
python3 poller.py [--source site_pm] [--dry-run] [--limit N] [--days-back N]
```

| Флаг | Описание |
|---|---|
| `--source` | Конкретный источник (по умолчанию все) |
| `--dry-run` | Показать найденные диалоги без обработки |
| `--limit N` | Обработать не более N диалогов |
| `--days-back N` | При первом запуске — сколько дней истории брать (default: 1) |

---

### `connectors/site_pm.py` — Коннектор личных сообщений

Подключается к MySQL (10.1.100.61, база `msg`).

**Логика завершённого диалога:**
- `last_msg_at > 1 час назад` (PM_INACTIVITY_HOURS=1)
- И `thread_user.is_unread = 0` у хотя бы одного организатора треда

**Организаторы:**
```sql
SELECT user_id FROM user.user_group
WHERE group_id = 58
  AND user_id NOT IN (SELECT user_id FROM user.user_group WHERE group_id IN (4, 5))
```

**Возвращает:** список объектов `Dialog` из `connectors/base.py`

---

### `connectors/base.py` — Базовый формат диалога

Dataclass `Dialog` — единый формат для всех источников:

```python
@dataclass
class Dialog:
    source: str          # 'jivo' | 'site_pm' | ...
    dialog_id: str       # уникальный ID внутри источника
    chat_id: int         # числовой ID (для CH)
    finished_at: str     # ISO datetime последнего сообщения
    visitor_name: str    # имя клиента
    visitor_id: str      # ID клиента
    chats_count: int     # кол-во диалогов клиента
    operator_name: str   # имя оператора
    page_url: str        # ссылка на контекст (закупку, страницу)
    plain_messages: str  # "Имя: текст\nОператор: текст\n..."
    raw_json: str        # оригинальные данные в JSON
```

Метод `to_payload()` приводит к формату `analyze_and_save()`.

---

### `report.py` — AI-отчёты

Формирует отчёт по периоду, отправляет в Telegram.

```bash
python3 report.py [--period yesterday|day|week] [--dry-run]
```

**Периоды:**

| `--period` | Текущий период | Предыдущий (для сравнения) |
|---|---|---|
| `yesterday` | Вчера | Позавчера |
| `day` | Сегодня | Вчера |
| `week` | Текущая неделя (пн–сегодня) | Прошлая неделя |

**Что собирается из CH:**
- Общие счётчики: всего, эмоции, статусы решения
- Топ-8 категорий с примерами проблем
- Бизнес-сигналы
- Эскалации и высокий churn
- Операторы с низкой оценкой (avg < 70, ≥ 2 диалогов)

---

### `sla_checker.py` — SLA-контроль

Считает скорость первого ответа, фиксирует нарушения в `dialog_sla`.

```bash
python3 sla_checker.py [--mode completed|open] [--source jivo|site_pm|all] [--days-back N] [--dry-run]
```

**Режимы:**

| `--mode` | Описание |
|---|---|
| `completed` | Завершённые диалоги: считает response_minutes и sla_violated |
| `open` | Активные треды без ответа (site_pm): записывает is_open=1 если SLA истёк |

**SLA правила по источникам:**

*site_pm* (серверное время, Пн–Пт 9:00–18:00):

| Время обращения | Дедлайн |
|---|---|
| Пн–Пт до 18:00 | 18:00 того же дня |
| Пн–Пт после 18:00 | 11:00 следующего рабочего дня |
| Пт после 18:00 / Сб / Вс | 11:00 понедельника |

*Jivo* (Самара UTC+4, Пн–Пт 8:00–20:00, норматив 5 минут):

| Время обращения | Дедлайн |
|---|---|
| Пн–Пт 8:00–20:00 | +5 минут |
| Пн–Пт после 20:00 | следующий рабочий день 08:05 |
| Пн–Пт до 8:00 | сегодня 08:05 |
| Пт после 20:00 / Сб / Вс | понедельник 08:05 |

---

## Схема ClickHouse

### `raw_dialogs`
Сырые данные — полный payload как JSON-строка.

```sql
source        LowCardinality(String)
event_name    String
chat_id       UInt64
payload_json  String
received_at   DateTime DEFAULT now()
```

### `dialogs`
Структурированные поля диалога.

```sql
source                  LowCardinality(String) DEFAULT 'jivo'
event_name              String
event_timestamp         DateTime
chat_id                 UInt64
widget_id               String
visitor_id              UInt64
visitor_name            String
visitor_chats_count     UInt32
operator_id             Nullable(UInt64)
operator_name           String
page_url                String
page_title              String
geo_country             String
geo_region              String
geo_city                String
chat_messages_json      String   -- JSON-массив сообщений (только Jivo)
invite_timestamp        Nullable(DateTime)
chat_rate               Nullable(Int8)
plain_messages          String   -- "Имя: текст\n..."
full_dialog_text        String
visitor_messages_text   String
agent_messages_text     String
```

### `dialog_analysis`
Результаты AI-анализа.

```sql
source                  LowCardinality(String) DEFAULT 'jivo'
chat_id                 UInt64
source_type             LowCardinality(String)
contact_reason          LowCardinality(String)
category                LowCardinality(String)
subcategory             LowCardinality(String)
user_problem_summary    String
user_emotion            LowCardinality(String)
churn_risk_score        Nullable(Float32)
resolution_status       LowCardinality(String)
needs_escalation        UInt8
agent_quality_score     UInt8
agent_quality_label     LowCardinality(String)
agent_quality_comment   String
business_signal         LowCardinality(String)
root_cause_guess        String
insight_comment         String
model_name              String
raw_llm_json            String
analyzed_at             DateTime DEFAULT now()
```

### `dialog_sla`
SLA-контроль. ReplacingMergeTree — дедупликация по `(source, chat_id)`.

```sql
source              LowCardinality(String)
chat_id             UInt64
client_msg_at       DateTime
operator_msg_at     Nullable(DateTime)
sla_deadline        DateTime
response_minutes    Nullable(Int32)
sla_violated        UInt8        -- 1 = нарушение
is_open             UInt8        -- 1 = тред ещё открыт
calculated_at       DateTime DEFAULT now()
-- ENGINE = ReplacingMergeTree(calculated_at) ORDER BY (source, chat_id)
```

### `poller_cursor`
Курсор поллера — последний обработанный timestamp.

```sql
source     String
last_seen  DateTime DEFAULT '1970-01-01 00:00:00'
-- ENGINE = ReplacingMergeTree ORDER BY source
```

---

## Конфигурация (.env)

```bash
# ClickHouse
CH_HOST=localhost
CH_PORT=8123
CH_USER=default
CH_PASSWORD=your_password_here
CH_DATABASE=default

# AI
AI_PROXY_URL=https://naitislova.ru/_ai/proxy
AI_API_KEY=your_api_key_here
AI_MODEL=openai/gpt-4o-mini
AI_TEMPERATURE=0.4
AI_MAX_TOKENS=2000
AI_MAX_RETRIES=3

# Telegram (для отчётов)
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
REPORT_MAX_TOKENS=4000

# MySQL — личные сообщения сайта
PM_DB_HOST=10.1.100.61
PM_DB_PORT=3306
PM_DB_USER=dev
PM_DB_PASSWORD=dev
PM_DB_NAME=msg
PM_USER_DB=user
PM_INACTIVITY_HOURS=1        # считать завершённым через N часов

# SLA — site_pm (Пн–Пт, серверное время)
SLA_START_HOUR=9
SLA_END_HOUR=18
SLA_RESPONSE_HOUR=11         # дедлайн следующего рабочего дня

# SLA — Jivo (Самара UTC+4, Пн–Пт 8–20, норматив 5 мин)
JIVO_SLA_MINUTES=5
JIVO_SLA_START_HOUR=8
JIVO_SLA_END_HOUR=20
JIVO_TZ_OFFSET_HOURS=4       # UTC+4 Самара
```

---

## Деплой и запуск

### Структура на сервере

```
/opt/jivo_inspector/
├── .env
├── venv/
├── main.py
├── ai_processor.py
├── poller.py
├── report.py
├── sla_checker.py
├── reprocess.py
└── connectors/
    ├── __init__.py
    ├── base.py
    └── site_pm.py

/srv/jivo-webhook-inspector/   ← git репозиторий
```

### Обновление на сервере

```bash
cd /srv/jivo-webhook-inspector && git pull --ff-only
cp main.py ai_processor.py poller.py report.py sla_checker.py reprocess.py /opt/jivo_inspector/
cp connectors/*.py /opt/jivo_inspector/connectors/
systemctl restart jivo_inspector
```

### Systemd (webhook-сервер)

```ini
# /etc/systemd/system/jivo_inspector.service
[Unit]
Description=JivoChat Webhook Inspector
After=network.target

[Service]
User=root
WorkingDirectory=/opt/jivo_inspector
EnvironmentFile=-/opt/jivo_inspector/.env
ExecStart=/opt/jivo_inspector/venv/bin/uvicorn main:app --host 0.0.0.0 --port 62000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable jivo_inspector
systemctl restart jivo_inspector
journalctl -u jivo_inspector -f
```

### Cron

> **Статус:** cron на сервере ещё не настроен. Все задачи запускаются вручную.
> Добавить в `crontab -e`:

```cron
# Поллер site_pm — каждые 15 минут
*/15 * * * * cd /opt/jivo_inspector && venv/bin/python3 poller.py --source site_pm >> logs/poller.log 2>&1

# SLA завершённых — каждый час
0 * * * * cd /opt/jivo_inspector && venv/bin/python3 sla_checker.py --mode completed >> logs/sla.log 2>&1

# SLA открытых — каждые 30 минут
*/30 * * * * cd /opt/jivo_inspector && venv/bin/python3 sla_checker.py --mode open >> logs/sla.log 2>&1

# Ежедневный отчёт — в 9:00
0 9 * * * cd /opt/jivo_inspector && venv/bin/python3 report.py --period yesterday >> logs/report.log 2>&1

# Еженедельный отчёт — понедельник в 9:05
5 9 * * 1 cd /opt/jivo_inspector && venv/bin/python3 report.py --period week >> logs/report.log 2>&1
```

---

## Добавление нового источника данных

1. Создать `connectors/my_source.py` с функцией:
   ```python
   def fetch_finished_dialogs(since: Optional[datetime] = None) -> list[Dialog]:
       ...
   ```
2. Зарегистрировать в `poller.py`:
   ```python
   SOURCES = {
       "site_pm":   "connectors.site_pm",
       "my_source": "connectors.my_source",  # ← добавить
   }
   ```
3. Добавить SLA-логику в `sla_checker.py` по аналогии с `process_pm_completed()`.

---

## Повторная обработка (reprocess.py)

Позволяет перепрогнать AI-анализ по уже сохранённым диалогам.

```bash
# Все необработанные диалоги
python3 reprocess.py

# Только конкретный источник
python3 reprocess.py --source site_pm

# Последние N диалогов
python3 reprocess.py --limit 50

# Посмотреть без записи
python3 reprocess.py --dry-run
```

---

## Зависимости

```
fastapi==0.115.0
uvicorn[standard]==0.30.6
pymysql          # pip install pymysql  (для site_pm)
```

ClickHouse, AI и Telegram взаимодействуют через stdlib `urllib` — без внешних зависимостей.
