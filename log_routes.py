"""
Журнал обращений — аналог Google Sheets.
Данные берутся из dialogs + dialog_analysis + raw_dialogs.
Редактируемые поля (Организатор, Ответственный, Результат, Комментарий)
хранятся в support_log_edits (ReplacingMergeTree).

Маршруты:
  GET  /log                  — HTML-страница
  GET  /api/log              — JSON: список строк + список операторов
  POST /api/log/{chat_id}    — сохранить правки
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

router = APIRouter()

CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")


def _params():
    return urllib.parse.urlencode({
        "user": CH_USER, "password": CH_PASSWORD, "database": CH_DATABASE,
    })


def ch_query(sql: str) -> list:
    url = f"http://{CH_HOST}:{CH_PORT}/?{_params()}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"CH {e.code}: {e.read().decode()[:300]}")
    rows = []
    for line in resp.read().decode().strip().splitlines():
        if line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def ch_execute(sql: str, data: bytes = None):
    if data is not None:
        # INSERT: SQL в URL query-параметре, данные в теле
        params = urllib.parse.urlencode({
            "query": sql,
            "user": CH_USER, "password": CH_PASSWORD, "database": CH_DATABASE,
        })
        url  = f"http://{CH_HOST}:{CH_PORT}/?{params}"
        body = data
    else:
        url  = f"http://{CH_HOST}:{CH_PORT}/?{_params()}"
        body = sql.encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"CH {e.code}: {e.read().decode()[:300]}")


def ensure_table():
    """Создаёт вспомогательные таблицы если их нет, добавляет новые колонки."""
    ch_execute("""
        CREATE TABLE IF NOT EXISTS support_log_edits (
            chat_id     UInt64,
            updated_at  DateTime DEFAULT now(),
            organizer   String   DEFAULT '',
            responsible String   DEFAULT '',
            result      String   DEFAULT '',
            comment     String   DEFAULT ''
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY chat_id
    """)
    for col in ["source_type", "category", "subcategory"]:
        try:
            ch_execute(f"ALTER TABLE support_log_edits ADD COLUMN IF NOT EXISTS {col} String DEFAULT ''")
        except Exception:
            pass
    ensure_manual_table()


def ensure_manual_table():
    """Временная таблица для ручных строк журнала (до интеграции новых каналов)."""
    ch_execute("""
        CREATE TABLE IF NOT EXISTS manual_log_entries (
            id              UInt64,
            updated_at      DateTime DEFAULT now(),
            date            Date     DEFAULT toDate(now()),
            time            String   DEFAULT '',
            channel         String   DEFAULT 'Другой',
            operator        String   DEFAULT '',
            source_type     String   DEFAULT '',
            author          String   DEFAULT '',
            appeal_type     String   DEFAULT '',
            category        String   DEFAULT '',
            subcategory     String   DEFAULT '',
            problem_summary String   DEFAULT '',
            result          String   DEFAULT '',
            comment         String   DEFAULT ''
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY id
    """)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_q = lambda s: s.replace("'", "\\'")


def get_manual_rows(df: str, dt: str, operator: str = "", channel: str = "",
                    stype: str = "", category: str = "", subcategory: str = "",
                    result: str = "") -> list:
    """Загружает ручные строки из manual_log_entries за период."""
    try:
        where = f"toDate(date) BETWEEN '{df}' AND '{dt}'"
        if operator:    where += f" AND operator    = '{_q(operator)}'"
        if channel:     where += f" AND channel     = '{_q(channel)}'"
        if stype:       where += f" AND source_type = '{_q(stype)}'"
        if category:    where += f" AND category    = '{_q(category)}'"
        if subcategory: where += f" AND subcategory = '{_q(subcategory)}'"
        if result:      where += f" AND result      = '{_q(result)}'"

        rows = ch_query(f"""
            SELECT
                concat('m_', toString(id)) AS row_key,
                0                          AS chat_id,
                toString(date)             AS date,
                time, operator, source_type, author,
                ''                         AS login,
                appeal_type, category, subcategory,
                problem_summary, result, comment, channel
            FROM manual_log_entries FINAL
            WHERE {where}
            ORDER BY date DESC, time DESC
            FORMAT JSONEachRow
        """)
        for r in rows:
            r["is_manual"] = True
        return rows
    except Exception:
        return []

    ch_execute("""
        CREATE TABLE IF NOT EXISTS day_tracker_edits (
            chat_id          UInt64,
            updated_at       DateTime DEFAULT now(),
            responsible_dept String   DEFAULT ''
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY chat_id
    """)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

PER_PAGE = 100

@router.get("/api/log")
def api_log(
    date_from:   str = Query(default=None),
    date_to:     str = Query(default=None),
    operator:    str = Query(default=""),
    channel:     str = Query(default=""),
    stype:       str = Query(default=""),
    category:    str = Query(default=""),
    subcategory: str = Query(default=""),
    result:      str = Query(default=""),
    page:        int = Query(default=1, ge=1),
):
    df = date_from or str(date.today() - timedelta(days=7))
    dt = date_to   or str(date.today())

    def q(s): return s.replace("'", "\\'")

    where = f"toDate(r.ts) BETWEEN '{df}' AND '{dt}'"
    if operator:
        where += f" AND d.operator_name = '{q(operator)}'"
    if channel == "Чат":
        where += " AND d.source = 'jivo'"
    elif channel == "ЛС":
        where += " AND d.source = 'site_pm'"
    # Фильтры по вычисляемым полям (с учётом правок из support_log_edits)
    if stype:
        where += f" AND if(e.source_type!=''  , e.source_type , ifNull(a.source_type,''))  = '{q(stype)}'"
    if category:
        where += f" AND if(e.category!=''     , e.category    , ifNull(a.category,''))     = '{q(category)}'"
    if subcategory:
        where += f" AND if(e.subcategory!=''  , e.subcategory , ifNull(a.subcategory,''))  = '{q(subcategory)}'"
    if result:
        where += f" AND if(e.result!='' AND e.result IS NOT NULL, e.result, ifNull(a.resolution_status,'')) = '{q(result)}'"

    offset = (page - 1) * PER_PAGE

    # Общее количество для пагинации
    total_rows = ch_query(f"""
        SELECT count() AS total
        FROM dialogs d
        JOIN (
            SELECT chat_id, max(received_at) AS ts
            FROM raw_dialogs
            WHERE event_name = 'chat_finished'
            GROUP BY chat_id
        ) r ON d.chat_id = r.chat_id
        WHERE {where}
        FORMAT JSONEachRow
    """)
    total = int((total_rows[0] if total_rows else {}).get("total", 0))

    rows = ch_query(f"""
        SELECT
            d.chat_id                                                              AS chat_id,
            toString(d.chat_id)                                                    AS row_key,
            toString(toDate(r.ts))                                                 AS date,
            substring(toString(r.ts), 12, 5)                                       AS time,
            ifNull(d.operator_name, '')                                            AS operator,
            ifNull(d.visitor_name, '')                                             AS author,
            toString(ifNull(d.visitor_id, 0))                                      AS login,
            ifNull(a.contact_reason, '')                                           AS appeal_type,
            if(e.source_type  != '', e.source_type,  ifNull(a.source_type, ''))   AS source_type,
            if(e.category     != '', e.category,     ifNull(a.category, ''))      AS category,
            if(e.subcategory  != '', e.subcategory,  ifNull(a.subcategory, ''))   AS subcategory,
            ifNull(a.user_problem_summary, '')                                     AS problem_summary,
            if(e.result IS NOT NULL AND e.result != '',
               e.result,
               ifNull(a.resolution_status, ''))                                    AS result,
            ifNull(e.comment, '')                                                  AS comment,
            multiIf(d.source='jivo','Чат',d.source='site_pm','ЛС',d.source)       AS channel
        FROM dialogs d
        JOIN (
            SELECT chat_id, max(received_at) AS ts
            FROM raw_dialogs
            WHERE event_name = 'chat_finished'
            GROUP BY chat_id
        ) r ON d.chat_id = r.chat_id
        LEFT JOIN dialog_analysis a ON d.chat_id = a.chat_id
        LEFT JOIN (SELECT * FROM support_log_edits FINAL) e ON d.chat_id = e.chat_id
        WHERE {where}
        ORDER BY r.ts DESC
        LIMIT {PER_PAGE} OFFSET {offset}
        FORMAT JSONEachRow
    """)
    for r in rows:
        r["is_manual"] = False

    # Ручные строки — только на первой странице, поверх основных
    manual_rows = []
    if page == 1:
        manual_rows = get_manual_rows(df, dt, operator, channel, stype, category, subcategory, result)
    all_rows = manual_rows + rows

    operators = ch_query("""
        SELECT DISTINCT operator_name
        FROM dialogs
        WHERE operator_name != '' AND operator_name IS NOT NULL
        ORDER BY operator_name
        FORMAT JSONEachRow
    """)

    manual_total = len(get_manual_rows(df, dt, operator, channel, stype, category, subcategory, result)) if page != 1 else len(manual_rows)

    return JSONResponse({
        "rows":      all_rows,
        "operators": [r["operator_name"] for r in operators],
        "total":     total + manual_total,
        "page":      page,
        "per_page":  PER_PAGE,
        "pages":     max(1, -(-total // PER_PAGE)),  # ceil division
    })


@router.get("/api/log/dialog/{chat_id}")
def api_dialog(chat_id: int):
    rows = ch_query(f"""
        SELECT chat_messages_json,
               ifNull(visitor_name, '')  AS visitor_name,
               ifNull(operator_name, '') AS operator_name
        FROM dialogs
        WHERE chat_id = {chat_id}
        LIMIT 1
        FORMAT JSONEachRow
    """)
    if not rows:
        return JSONResponse({"messages": [], "visitor": "", "operator": ""})
    r = rows[0]
    try:
        messages = json.loads(r.get("chat_messages_json") or "[]")
    except Exception:
        messages = []
    return JSONResponse({
        "messages": messages,
        "visitor":  r.get("visitor_name", ""),
        "operator": r.get("operator_name", ""),
    })


@router.post("/api/log/manual")
def api_create_manual():
    """Создаёт новую пустую строку вручную."""
    ensure_manual_table()
    import time as _time
    new_id = int(_time.time() * 1000)
    today  = str(date.today())
    ch_execute(
        "INSERT INTO manual_log_entries (id, date, channel) FORMAT JSONEachRow",
        data=json.dumps({"id": new_id, "date": today, "channel": "Другой"}).encode("utf-8"),
    )
    return JSONResponse({"ok": True, "id": new_id, "date": today})


class ManualEditPayload(BaseModel):
    date:            Optional[str] = ""
    time:            Optional[str] = ""
    channel:         Optional[str] = ""
    operator:        Optional[str] = ""
    source_type:     Optional[str] = ""
    author:          Optional[str] = ""
    appeal_type:     Optional[str] = ""
    category:        Optional[str] = ""
    subcategory:     Optional[str] = ""
    problem_summary: Optional[str] = ""
    result:          Optional[str] = ""
    comment:         Optional[str] = ""


@router.delete("/api/log/manual/{row_id}")
def api_delete_manual(row_id: int):
    ch_execute(f"ALTER TABLE manual_log_entries DELETE WHERE id = {row_id}")
    return JSONResponse({"ok": True})


@router.post("/api/log/manual/{row_id}")
def api_edit_manual(row_id: int, payload: ManualEditPayload):
    today = str(date.today())
    row = json.dumps({
        "id":            row_id,
        "date":          payload.date or today,
        "time":          payload.time or "",
        "channel":       payload.channel or "Другой",
        "operator":      payload.operator or "",
        "source_type":   payload.source_type or "",
        "author":        payload.author or "",
        "appeal_type":   payload.appeal_type or "",
        "category":      payload.category or "",
        "subcategory":   payload.subcategory or "",
        "problem_summary": payload.problem_summary or "",
        "result":        payload.result or "",
        "comment":       payload.comment or "",
    }, ensure_ascii=False)
    ch_execute(
        "INSERT INTO manual_log_entries "
        "(id, date, time, channel, operator, source_type, author, appeal_type, "
        "category, subcategory, problem_summary, result, comment) "
        "FORMAT JSONEachRow",
        data=row.encode("utf-8"),
    )
    return JSONResponse({"ok": True})


class EditPayload(BaseModel):
    organizer:   Optional[str] = ""
    responsible: Optional[str] = ""
    result:      Optional[str] = ""
    comment:     Optional[str] = ""
    source_type: Optional[str] = ""
    category:    Optional[str] = ""
    subcategory: Optional[str] = ""


@router.post("/api/log/{chat_id}")
def api_edit(chat_id: int, payload: EditPayload):
    row = json.dumps({
        "chat_id":     chat_id,
        "organizer":   payload.organizer   or "",
        "responsible": payload.responsible or "",
        "result":      payload.result      or "",
        "comment":     payload.comment     or "",
        "source_type": payload.source_type or "",
        "category":    payload.category    or "",
        "subcategory": payload.subcategory or "",
    }, ensure_ascii=False)
    ch_execute(
        "INSERT INTO support_log_edits "
        "(chat_id, organizer, responsible, result, comment, source_type, category, subcategory) "
        "FORMAT JSONEachRow",
        data=row.encode("utf-8"),
    )
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

@router.get("/log", response_class=HTMLResponse)
def log_page():
    return _HTML


_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Журнал обращений</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body {
  height: 100%; overflow: hidden;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 13px; background: #f0f2f5; color: #222;
  display: flex; flex-direction: column;
}

/* ── Toolbar ── */
.toolbar {
  flex-shrink: 0;
  background: #fff; border-bottom: 1px solid #ddd;
  padding: 8px 14px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
}
.toolbar h1 { font-size: 15px; font-weight: 600; color: #333; margin-right: 6px; }
.filter-group { display: flex; align-items: center; gap: 4px; }
.filter-group label { font-size: 12px; color: #666; white-space: nowrap; }
.toolbar input[type=date], .toolbar select {
  border: 1px solid #ccc; border-radius: 5px; padding: 4px 7px;
  font-size: 12px; background: #fafafa; color: #333;
  height: 28px;
}
.toolbar input[type=date]:focus, .toolbar select:focus {
  outline: none; border-color: #1a73e8; background: #fff;
}
.btn {
  background: #1a73e8; color: #fff; border: none; border-radius: 5px;
  padding: 0 14px; height: 28px; cursor: pointer; font-size: 13px; font-weight: 500;
  white-space: nowrap;
}
.btn:hover { background: #1557b0; }
.count { margin-left: auto; font-size: 12px; color: #888; white-space: nowrap; }

/* ── Table wrapper ── */
.wrap {
  flex: 1;
  overflow: auto;   /* скролл обоих осей внутри контейнера */
  padding: 0 0 10px 0;  /* без отступа сверху — заголовки вплотную к тулбару */
}

table {
  border-collapse: collapse;
  width: max-content; min-width: 100%;
  background: #fff;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
}

thead th {
  background: #f1f3f4;
  border-bottom: 2px solid #ddd;
  border-right: 1px solid #e0e0e0;
  padding: 8px 10px;
  text-align: left;
  font-weight: 600;
  font-size: 12px;
  color: #444;
  white-space: nowrap;
  position: sticky;
  top: 0;           /* прилипает к верху .wrap, а не вьюпорта */
  z-index: 10;
}
thead th:last-child { border-right: none; }

tbody tr { border-bottom: 1px solid #f0f0f0; }
tbody tr:last-child { border-bottom: none; }
tbody tr:hover { background: #f8fbff; }

tbody td {
  padding: 6px 10px;
  border-right: 1px solid #f0f0f0;
  vertical-align: top;
}
tbody td:last-child { border-right: none; }

/* Column widths */
.col-date    { white-space: nowrap; }
.col-time    { white-space: nowrap; color: #888; }
.col-channel { white-space: nowrap; font-weight: 500; }
.col-author  { max-width: 90px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* ── Ресайз столбцов ── */
.col-resize-handle {
  position: absolute; right: 0; top: 0;
  width: 5px; height: 100%;
  cursor: col-resize;
  user-select: none;
  z-index: 1;
}
.col-resize-handle:hover,
.col-resize-handle.dragging { background: #1a73e8; }
.col-summary { max-width: 320px; word-wrap: break-word; line-height: 1.4; }
.col-login   { color: #888; font-size: 12px; }
.col-week    { text-align: center; color: #888; font-size: 12px; }

/* Channel colors */
.ch-chat { color: #1a73e8; }
.ch-ls   { color: #0f9d58; }

/* Editable cells */
.editable {
  display: block;
  min-width: 80px;
  min-height: 20px;
  padding: 3px 5px;
  border-radius: 4px;
  outline: none;
  cursor: text;
  white-space: pre-wrap;
  word-break: break-word;
  overflow-wrap: break-word;
  transition: background .15s, box-shadow .15s;
}
.editable:empty::before {
  content: attr(data-placeholder);
  color: #bbb;
  font-style: italic;
  pointer-events: none;
}
.editable:empty { cursor: pointer; }
.editable:focus {
  background: #fffbea;
  box-shadow: 0 0 0 2px #f59e0b55;
}
.editable.saving { opacity: .6; }
.editable.saved  { animation: flash 1s ease forwards; }
.select-cell {
  cursor: pointer; padding: 3px 5px; border-radius: 4px;
  min-width: 60px; min-height: 20px;
  transition: background .15s;
}
.select-cell:hover { background: #f0f7ff; }
.select-cell.saved { animation: flash 1s ease forwards; }
.select-cell select {
  width: 100%; font-size: 12px; border: 1px solid #fbbf24;
  border-radius: 3px; padding: 2px 4px; background: #fffbea;
  cursor: pointer;
}
@keyframes flash {
  0%   { background: #d1fae5; }
  100% { background: transparent; }
}

/* Result badge */
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 11px; font-weight: 600; white-space: nowrap;
}
.badge-решено     { background: #d1fae5; color: #065f46; }
.badge-не-решено  { background: #fee2e2; color: #991b1b; }
.badge-частично   { background: #fef3c7; color: #92400e; }
.badge-эскалация  { background: #ede9fe; color: #5b21b6; }
.badge-other      { background: #f3f4f6; color: #374151; }

/* Problem summary truncation */
.summary-text {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
  cursor: pointer;
}
.summary-text.expanded {
  display: block;
  -webkit-line-clamp: unset;
}

.no-data { text-align: center; padding: 60px 20px; color: #aaa; font-size: 14px; }

/* Manual rows */
tr[data-manual="1"] { background: #fffdf0; }
tr[data-manual="1"]:hover { background: #fff8d6; }
tr[data-manual="1"] td:first-child { border-left: 3px solid #f59e0b; }

.btn-green { background: #0f9d58; }
.btn-green:hover { background: #0b7a43; }

.del-btn {
  float: right; margin-left: 4px;
  background: none; border: none;
  color: #ccc; cursor: pointer;
  font-size: 15px; line-height: 1; padding: 0 2px;
}
.del-btn:hover { color: #e53e3e; }

/* Expand dialog */
.expand-btn {
  cursor: pointer; color: #ccc; font-size: 9px;
  margin-right: 4px; display: inline-block;
  transition: transform .15s, color .15s;
  user-select: none; vertical-align: middle;
}
.expand-btn:hover { color: #1a73e8; }
.expand-btn.open { transform: rotate(90deg); color: #1a73e8; }

tr.dialog-row td { padding: 0 !important; background: #f8f9fa; border-bottom: 2px solid #e0e0e0; }
.dialog-wrap { padding: 14px 20px; max-height: 420px; overflow-y: auto; display: flex; flex-direction: column; gap: 6px; }
.d-msg { display: flex; }
.d-msg.visitor { padding-left: 16px; }
.d-msg.agent   { padding-left: 0; }
.d-bubble-wrap { max-width: 65%; }
.d-name { font-size: 10px; color: #999; margin-bottom: 2px; }
.d-bubble {
  padding: 7px 11px; border-radius: 12px;
  font-size: 12px; line-height: 1.5; word-break: break-word; white-space: pre-wrap;
}
.d-bubble.visitor { background: #1a73e8; color: #fff; border-bottom-left-radius: 3px; }
.d-bubble.agent   { background: #fff; border: 1px solid #ddd; color: #333; border-bottom-left-radius: 3px; }
.d-time { font-size: 10px; color: #bbb; margin-top: 3px; }
.d-empty { color: #aaa; font-style: italic; font-size: 12px; padding: 8px 0; }
.spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #ddd; border-top-color: #1a73e8; border-radius: 50%; animation: spin .6s linear infinite; vertical-align: middle; margin-right: 6px; }
@keyframes spin { to { transform: rotate(360deg); } }

/* ── Пагинация ── */
.pagination {
  flex-shrink: 0;
  display: flex; align-items: center; justify-content: center; gap: 4px;
  padding: 8px; background: #fff; border-top: 1px solid #e8e8e8;
}
.pg-btn {
  min-width: 32px; height: 28px; padding: 0 8px;
  border: 1px solid #ddd; border-radius: 4px; background: #fff;
  font-size: 12px; cursor: pointer; color: #333;
  display: flex; align-items: center; justify-content: center;
}
.pg-btn:hover:not(:disabled) { background: #f0f7ff; border-color: #1a73e8; color: #1a73e8; }
.pg-btn.active { background: #1a73e8; color: #fff; border-color: #1a73e8; font-weight: 600; }
.pg-btn:disabled { opacity: .4; cursor: default; }
.pg-info { font-size: 12px; color: #888; padding: 0 8px; }
</style>
</head>
<body>

<div class="toolbar">
  <h1>📋 Журнал обращений</h1>

  <div class="filter-group">
    <label>С</label>
    <input type="date" id="date_from">
  </div>
  <div class="filter-group">
    <label>По</label>
    <input type="date" id="date_to">
  </div>
  <div class="filter-group">
    <label>Оператор</label>
    <select id="filter_operator"><option value="">Все</option></select>
  </div>
  <div class="filter-group">
    <label>Канал</label>
    <select id="filter_channel">
      <option value="">Все</option>
      <option value="Чат">Чат (Jivo)</option>
      <option value="ЛС">ЛС (сайт)</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Тип автора</label>
    <select id="filter_stype">
      <option value="">Все</option>
      <option value="Клиент">Клиент</option>
      <option value="ПВЗ">ПВЗ</option>
      <option value="Сотрудник">Сотрудник</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Категория</label>
    <select id="filter_category" onchange="updateSubcatFilter()">
      <option value="">Все</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Подкатегория</label>
    <select id="filter_subcategory"><option value="">Все</option></select>
  </div>
  <div class="filter-group">
    <label>Результат</label>
    <select id="filter_result">
      <option value="">Все</option>
      <option value="Решено">Решено</option>
      <option value="Не решено">Не решено</option>
      <option value="Частично">Частично</option>
      <option value="Эскалация">Эскалация</option>
    </select>
  </div>
  <button class="btn" onclick="load()">Применить</button>
  <button class="btn btn-green" id="btn_add" onclick="addManualRow()" title="Добавить строку вручную">+ Строка</button>
  <span class="count" id="count"></span>
</div>

<div class="wrap">
  <table>
    <thead>
      <tr>
        <th>Дата</th>
        <th>Время</th>
        <th>Оператор</th>
        <th>Тип автора</th>
        <th>Автор</th>
        <th>Логин</th>
        <th>Тип</th>
        <th>Категория</th>
        <th>Подкатегория</th>
        <th>Причина обращения</th>
        <th>Результат</th>
        <th>Комментарий</th>
        <th>Канал</th>
        <th>№ обращения</th>
      </tr>
    </thead>
    <tbody id="tbody">
      <tr><td colspan="14" class="no-data"><span class="spinner"></span>Загрузка...</td></tr>
    </tbody>
  </table>
</div>

<div class="pagination" id="pagination"></div>

<script>
// Defaults: last 7 days
const today = new Date();
const week  = new Date(today); week.setDate(today.getDate() - 7);
document.getElementById('date_from').value = fmt(week);
document.getElementById('date_to').value   = fmt(today);

let currentPage = 1;

function fmt(d) {
  return d.toISOString().split('T')[0];
}

function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function badgeClass(result) {
  const r = (result || '').toLowerCase();
  if (r === 'решено')    return 'badge-решено';
  if (r === 'не решено') return 'badge-не-решено';
  if (r === 'частично')  return 'badge-частично';
  if (r === 'эскалация') return 'badge-эскалация';
  return r ? 'badge-other' : '';
}

function initFilters() {
  const sel = document.getElementById('filter_category');
  sel.innerHTML = '<option value="">Все</option>' +
    Object.keys(CAT_MAP).map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('');
}

function updateSubcatFilter() {
  const cat = document.getElementById('filter_category').value;
  const sel = document.getElementById('filter_subcategory');
  const cur = sel.value;
  const subs = CAT_MAP[cat] || [];
  sel.innerHTML = '<option value="">Все</option>' +
    subs.map(s => `<option value="${esc(s)}"${s===cur?' selected':''}>${esc(s)}</option>`).join('');
}

function goPage(p) { currentPage = p; load(false); }

async function load(resetPage = true) {
  if (resetPage) currentPage = 1;

  const params = new URLSearchParams({
    date_from:   document.getElementById('date_from').value,
    date_to:     document.getElementById('date_to').value,
    operator:    document.getElementById('filter_operator').value,
    channel:     document.getElementById('filter_channel').value,
    stype:       document.getElementById('filter_stype').value,
    category:    document.getElementById('filter_category').value,
    subcategory: document.getElementById('filter_subcategory').value,
    result:      document.getElementById('filter_result').value,
    page:        currentPage,
  });

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '<tr><td colspan="14" class="no-data"><span class="spinner"></span>Загрузка...</td></tr>';
  document.getElementById('count').textContent = '';
  document.getElementById('pagination').innerHTML = '';

  try {
    const resp = await fetch('/api/log?' + params);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    // Обновляем список операторов (только при первой загрузке / сбросе)
    if (resetPage) {
      const sel = document.getElementById('filter_operator');
      const cur = sel.value;
      sel.innerHTML = '<option value="">Все</option>' +
        data.operators.map(o => `<option value="${esc(o)}"${o===cur?' selected':''}>${esc(o)}</option>`).join('');
    }

    render(data.rows);
    renderPagination(data.page, data.pages, data.total);
    document.getElementById('count').textContent =
      `${data.total} записей, стр. ${data.page} из ${data.pages}`;
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="14" class="no-data">Ошибка загрузки: ${e.message}</td></tr>`;
  }
}

function renderPagination(page, pages, total) {
  const el = document.getElementById('pagination');
  if (pages <= 1) { el.innerHTML = ''; return; }

  const btns = [];

  // Кнопка «Назад»
  btns.push(`<button class="pg-btn" onclick="goPage(${page-1})" ${page===1?'disabled':''}>‹</button>`);

  // Номера страниц (окно ±2 от текущей)
  const range = new Set([1, pages]);
  for (let i = Math.max(1, page-2); i <= Math.min(pages, page+2); i++) range.add(i);
  let prev = 0;
  for (const p of [...range].sort((a,b)=>a-b)) {
    if (prev && p - prev > 1) btns.push(`<span class="pg-info">…</span>`);
    btns.push(`<button class="pg-btn${p===page?' active':''}" onclick="goPage(${p})">${p}</button>`);
    prev = p;
  }

  // Кнопка «Вперёд»
  btns.push(`<button class="pg-btn" onclick="goPage(${page+1})" ${page===pages?'disabled':''}>›</button>`);

  el.innerHTML = btns.join('');
}

function render(rows) {
  const tbody = document.getElementById('tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="14" class="no-data">Нет данных за выбранный период</td></tr>';
    return;
  }

  tbody.innerHTML = rows.map(r => {
    const rowKey  = r.row_key !== undefined ? r.row_key : String(r.chat_id);
    const isManual = !!r.is_manual;
    const chClass = r.channel === 'Чат' ? 'ch-chat' : r.channel === 'ЛС' ? 'ch-ls' : '';
    const bc = badgeClass(r.result);

    const resultHtml = isManual
      ? `<div class="select-cell" data-field="result" data-value="${esc(r.result)}" onclick="openSelect(this)">${esc(r.result) || '<span style=color:#bbb>—</span>'}</div>`
      : (bc ? `<span class="badge ${bc}">${esc(r.result)}</span>` : (r.result ? esc(r.result) : '<span style="color:#bbb">—</span>'));

    const channelHtml = isManual
      ? `<div class="select-cell" data-field="channel" data-value="${esc(r.channel)}" onclick="openSelect(this)">${esc(r.channel) || '<span style=color:#bbb>—</span>'}</div>`
      : esc(r.channel);

    const dateHtml = isManual
      ? `<button class="del-btn" onclick="deleteManualRow(this,'${rowKey}')" title="Удалить строку">×</button><div class="editable" contenteditable="true" data-field="date" data-orig="${esc(r.date)}" data-placeholder="ГГГГ-ММ-ДД">${esc(r.date)}</div>`
      : `<span class="expand-btn" onclick="toggleDialog(event,'${rowKey}')">▶</span>${esc(r.date)}`;
    const timeHtml = isManual
      ? `<div class="editable" contenteditable="true" data-field="time" data-orig="${esc(r.time)}" data-placeholder="ЧЧ:ММ">${esc(r.time)}</div>`
      : esc(r.time);
    const operatorHtml = isManual
      ? `<div class="editable" contenteditable="true" data-field="operator" data-orig="${esc(r.operator)}" data-placeholder="Оператор">${esc(r.operator)}</div>`
      : esc(r.operator);
    const authorHtml = isManual
      ? `<div class="editable" contenteditable="true" data-field="author" data-orig="${esc(r.author)}" data-placeholder="Автор">${esc(r.author)}</div>`
      : esc(r.author);
    const appealHtml = isManual
      ? `<div class="editable" contenteditable="true" data-field="appeal_type" data-orig="${esc(r.appeal_type)}" data-placeholder="Тип">${esc(r.appeal_type)}</div>`
      : esc(r.appeal_type);
    const summaryHtml = isManual
      ? `<div class="editable" contenteditable="true" data-field="problem_summary" data-orig="${esc(r.problem_summary)}" data-placeholder="Суть обращения">${esc(r.problem_summary)}</div>`
      : `<div class="summary-text" onclick="this.classList.toggle('expanded')">${esc(r.problem_summary)}</div>`;

    return `<tr data-id="${rowKey}"${isManual ? ' data-manual="1"' : ''}>
      <td class="col-date">${dateHtml}</td>
      <td class="col-time">${timeHtml}</td>
      <td>${operatorHtml}</td>
      <td><div class="select-cell" data-field="source_type" data-value="${esc(r.source_type)}" onclick="openSelect(this)">${esc(r.source_type) || '<span style=color:#bbb>—</span>'}</div></td>
      <td class="col-author" title="${esc(r.author)}">${authorHtml}</td>
      <td class="col-login">${esc(extractLogin(r.author, r.source_type))}</td>
      <td>${appealHtml}</td>
      <td><div class="select-cell" data-field="category"    data-value="${esc(r.category)}"    onclick="openSelect(this)">${esc(r.category)    || '<span style=color:#bbb>—</span>'}</div></td>
      <td><div class="select-cell" data-field="subcategory" data-value="${esc(r.subcategory)}" onclick="openSelect(this)">${esc(r.subcategory) || '<span style=color:#bbb>—</span>'}</div></td>
      <td class="col-summary">${summaryHtml}</td>
      <td>${resultHtml}</td>
      <td><div class="editable" contenteditable="true" data-field="comment" data-orig="${esc(r.comment)}" data-placeholder="Добавить...">${esc(r.comment)}</div></td>
      <td class="col-channel ${chClass}">${channelHtml}</td>
      <td class="col-login" style="color:#aaa">${isManual ? '' : esc(String(r.chat_id))}</td>
    </tr>`;
  }).join('');

  tbody.querySelectorAll('.editable').forEach(el => {
    el.addEventListener('blur', onBlur);
  });
}

async function onBlur(e) {
  const el   = e.target;
  const row  = el.closest('tr');
  const orig = el.dataset.orig;
  const val  = el.textContent.trim();

  if (val === orig) return; // не изменилось

  // Очищаем <br> которые браузер оставляет при удалении текста,
  // чтобы CSS :empty::before (плейсхолдер) снова сработал
  if (val === '') el.innerHTML = '';

  const chatId = row.dataset.id;
  const fields = collectFields(row);

  el.dataset.orig = val;
  el.classList.add('saving');
  await saveRow(row, el);
  el.classList.remove('saving');
}

// ── Логин из имени автора ──────────────────────────────────────────────────
function extractLogin(author, sourceType) {
  if (!author) return '';
  // ПВЗ и Сотрудник — логин равен автору
  if (sourceType === 'ПВЗ' || sourceType === 'Сотрудник') return author;

  // 1. После запятой
  const ci = author.indexOf(',');
  if (ci !== -1) return author.slice(ci + 1).trim();

  // 2. После слова "лог" (лог, логин и т.п.)
  const li = author.toLowerCase().indexOf('лог');
  if (li !== -1) {
    const after = author.slice(li + 3).replace(/^[\s:]+/, '').trim();
    if (after) return after;
  }

  // 3. Последнее слово
  const words = author.trim().split(/\s+/);
  if (words.length) return words[words.length - 1];

  // 4. Сам автор
  return author;
}

// ── Собрать все редактируемые поля строки ──────────────────────────────────
function collectFields(row) {
  const f = {};
  row.querySelectorAll('.editable').forEach(c => {
    f[c.dataset.field] = c.textContent.trim();
  });
  row.querySelectorAll('.select-cell[data-field]').forEach(c => {
    f[c.dataset.field] = c.dataset.value || '';
  });
  return f;
}

// ── Сохранить строку ────────────────────────────────────────────────────────
async function saveRow(row, feedbackEl) {
  const rowKey = row.dataset.id;
  const fields = collectFields(row);

  const url = (rowKey && rowKey.startsWith('m_'))
    ? '/api/log/manual/' + rowKey.slice(2)
    : '/api/log/' + rowKey;

  try {
    const resp = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(fields),
    });
    if (resp.ok && feedbackEl) {
      feedbackEl.classList.add('saved');
      setTimeout(() => feedbackEl.classList.remove('saved'), 1200);
    }
  } catch (err) {
    console.error('Save error:', err);
  }
}

// ── Карта категорий / подкатегорий ─────────────────────────────────────────
const SOURCE_TYPES = ['Клиент', 'ПВЗ', 'Сотрудник', 'Жалоба', 'Заявка'];

const CAT_MAP = {
  'Заказ и доставка':      ['Статус заказа / задержка','Заказ не найден / потерян','Статус не обновился','Объединение / формирование посылки','Возврат товара (логистика)'],
  'ПВЗ и выдача':          ['График / адрес ПВЗ','Заказ не выдается','Проблема при приеме товара','Не сканируется / нет стикера','Ручные операции / обход процесса','Стать раздающим'],
  'Оплата и финансы':      ['Возврат денег','Непонятные списания / долг','Платное хранение','Оргсбор — размер и расчёт'],
  'Технические проблемы':  ['Ошибка / сбой сайта','Некорректное отображение','Поиск / каталог','Мобильное приложение'],
  'Аккаунт и профиль':     ['Нет доступа / ошибка входа','Смена телефона / почты','Блокировка / ограничения','Управление рассылками'],
  'Качество товара':       ['Брак / дефект','Несоответствие описанию или фото','Пересорт / пришло не то','Повреждение при доставке','Нарушения — маркировка, сроки годности'],
  'Закупки и организаторы':['Вопрос по закупке','Статус закупки / оплаты','Жалоба на организатора'],
  'Пристрой':              ['Как работает пристрой','Проблемы / передача пристроя'],
  'Пожелания и инсайты':   ['Запрос нового бренда / поставщика','Запрос открытия ПВЗ в регионе','Запрос новой функции платформы','Коммерческое предложение / партнёрство','Сравнение с конкурентами (WB, Ozon)'],
  'Благодарность':         ['Благодарность'],
  'Не определено':         ['Служебное / внутреннее'],
};

// ── Открыть выпадающий список ──────────────────────────────────────────────
function openSelect(cell) {
  if (cell.querySelector('select')) return; // уже открыт

  const field   = cell.dataset.field;
  const current = cell.dataset.value || '';
  const row     = cell.closest('tr');

  let options = [];
  if (field === 'source_type') {
    options = SOURCE_TYPES;
  } else if (field === 'category') {
    options = Object.keys(CAT_MAP);
  } else if (field === 'subcategory') {
    const catCell = row.querySelector('[data-field="category"]');
    const cat = catCell ? catCell.dataset.value : '';
    options = CAT_MAP[cat] || [];
  } else if (field === 'result') {
    options = ['Решено', 'Не решено', 'Частично', 'Эскалация'];
  } else if (field === 'channel') {
    options = ['Чат', 'ЛС', 'Email', 'Телефон', 'Другой'];
  }

  const sel = document.createElement('select');
  sel.innerHTML = '<option value="">— выбрать —</option>' +
    options.map(v => `<option value="${esc(v)}"${v === current ? ' selected' : ''}>${esc(v)}</option>`).join('');

  cell.innerHTML = '';
  cell.appendChild(sel);
  sel.focus();

  async function apply() {
    const val = sel.value;
    cell.dataset.value = val;
    cell.innerHTML = val
      ? esc(val)
      : '<span style="color:#bbb">—</span>';
    cell.onclick = () => openSelect(cell); // вернуть обработчик

    // Если сменили категорию — сбрасываем подкатегорию
    if (field === 'category') {
      const subCell = row.querySelector('[data-field="subcategory"]');
      if (subCell) {
        subCell.dataset.value = '';
        subCell.innerHTML = '<span style="color:#bbb">—</span>';
      }
    }

    await saveRow(row, cell);
  }

  sel.addEventListener('change', apply);
  sel.addEventListener('blur',   apply);
}

// ── Ресайз столбцов ────────────────────────────────────────────────────────
const COL_WIDTHS_KEY = 'log_col_widths_v1';

// Дефолтные ширины по порядку столбцов (px)
// Дата, Время, Оператор, Тип автора, Автор, Логин, Тип, Категория, Подкатегория, Причина обращения, Результат, Комментарий, Канал, № обращения
const DEFAULT_COL_WIDTHS = [90, 55, 90, 90, 100, 110, 90, 120, 140, 340, 90, 140, 55, 90];

function initResizableColumns() {
  const ths = [...document.querySelectorAll('thead th')];
  ths.forEach((th, i) => {
    const handle = document.createElement('div');
    handle.className = 'col-resize-handle';
    th.appendChild(handle);

    let startX, startW;

    handle.addEventListener('mousedown', e => {
      startX = e.pageX;
      startW = th.offsetWidth;
      handle.classList.add('dragging');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';

      const onMove = e => {
        const w = Math.max(30, startW + e.pageX - startX);
        th.style.width = th.style.minWidth = th.style.maxWidth = w + 'px';
      };
      const onUp = () => {
        handle.classList.remove('dragging');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        saveColWidths();
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
      e.preventDefault();
    });
  });

  restoreColWidths();
}

function saveColWidths() {
  const widths = [...document.querySelectorAll('thead th')]
    .map(th => th.style.width || '');
  localStorage.setItem(COL_WIDTHS_KEY, JSON.stringify(widths));
}

function restoreColWidths() {
  try {
    const saved = JSON.parse(localStorage.getItem(COL_WIDTHS_KEY) || '[]');
    const ths = [...document.querySelectorAll('thead th')];
    ths.forEach((th, i) => {
      const w = saved[i] || (DEFAULT_COL_WIDTHS[i] ? DEFAULT_COL_WIDTHS[i] + 'px' : null);
      if (w) th.style.width = th.style.minWidth = th.style.maxWidth = w;
    });
  } catch (_) {}
}

// ── Диалог (разворачивание строки) ──────────────────────────────────────────
const _dialogCache = {};

async function toggleDialog(e, rowKey) {
  e.stopPropagation();
  const btn = e.currentTarget;
  const tr  = btn.closest('tr');
  const next = tr.nextElementSibling;

  // Свернуть если уже открыт
  if (next && next.dataset.dialogFor === rowKey) {
    next.remove();
    btn.classList.remove('open');
    return;
  }

  btn.textContent = '…';
  btn.classList.remove('open');

  try {
    let data = _dialogCache[rowKey];
    if (!data) {
      const resp = await fetch('/api/log/dialog/' + rowKey);
      data = await resp.json();
      _dialogCache[rowKey] = data;
    }
    const expandTr = document.createElement('tr');
    expandTr.className = 'dialog-row';
    expandTr.dataset.dialogFor = rowKey;
    expandTr.innerHTML = `<td colspan="14"><div class="dialog-wrap">${renderDialog(data)}</div></td>`;
    tr.after(expandTr);
    btn.textContent = '▶';
    btn.classList.add('open');
  } catch(err) {
    btn.textContent = '▶';
    console.error('Dialog load error:', err);
  }
}

function fmtMsgTime(ts) {
  if (!ts) return '';
  const d = new Date(ts > 1e10 ? ts : ts * 1000);
  return d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}

function renderDialog(data) {
  const msgs = data.messages || [];
  if (!msgs.length) return `<div class="d-empty">Сообщения недоступны</div>`;
  return msgs.map(m => {
    const isV  = m.type === 'visitor';
    const cls  = isV ? 'visitor' : 'agent';
    const name = isV ? esc(data.visitor || 'Клиент') : esc(data.operator || 'Оператор');
    const text = esc((m.message || '').trim());
    const ts   = m.timestamp || m.ts || m.time || 0;
    const time = fmtMsgTime(ts);
    return `<div class="d-msg ${cls}">
      <div class="d-bubble-wrap">
        <div class="d-name">${name}</div>
        <div class="d-bubble ${cls}">${text}</div>
        ${time ? `<div class="d-time">${time}</div>` : ''}
      </div>
    </div>`;
  }).join('');
}

// ── Удаление ручной строки ──────────────────────────────────────────────────
async function deleteManualRow(btn, rowKey) {
  if (!confirm('Удалить строку?')) return;
  const id = rowKey.slice(2);
  try {
    const resp = await fetch('/api/log/manual/' + id, { method: 'DELETE' });
    if (resp.ok) btn.closest('tr').remove();
    else alert('Ошибка удаления');
  } catch(e) {
    alert('Ошибка: ' + e.message);
  }
}

// ── Добавление ручной строки ────────────────────────────────────────────────
async function addManualRow() {
  const btn = document.getElementById('btn_add');
  btn.disabled = true;
  try {
    const resp = await fetch('/api/log/manual', { method: 'POST' });
    const data = await resp.json();
    if (!data.ok) throw new Error('Ошибка создания');
    prependManualRow(data.id, data.date);
  } catch(e) {
    alert('Ошибка: ' + e.message);
  } finally {
    btn.disabled = false;
  }
}

function prependManualRow(id, dateStr) {
  const rowKey = 'm_' + id;
  const today  = dateStr || fmt(new Date());
  const tr = document.createElement('tr');
  tr.dataset.id     = rowKey;
  tr.dataset.manual = '1';
  tr.innerHTML = `
    <td class="col-date"><button class="del-btn" onclick="deleteManualRow(this,'${rowKey}')" title="Удалить строку">×</button><div class="editable" contenteditable="true" data-field="date" data-orig="${esc(today)}" data-placeholder="ГГГГ-ММ-ДД">${esc(today)}</div></td>
    <td class="col-time"><div class="editable" contenteditable="true" data-field="time" data-orig="" data-placeholder="ЧЧ:ММ"></div></td>
    <td><div class="editable" contenteditable="true" data-field="operator" data-orig="" data-placeholder="Оператор"></div></td>
    <td><div class="select-cell" data-field="source_type" data-value="" onclick="openSelect(this)"><span style="color:#bbb">—</span></div></td>
    <td class="col-author"><div class="editable" contenteditable="true" data-field="author" data-orig="" data-placeholder="Автор"></div></td>
    <td class="col-login"></td>
    <td><div class="editable" contenteditable="true" data-field="appeal_type" data-orig="" data-placeholder="Тип"></div></td>
    <td><div class="select-cell" data-field="category"    data-value="" onclick="openSelect(this)"><span style="color:#bbb">—</span></div></td>
    <td><div class="select-cell" data-field="subcategory" data-value="" onclick="openSelect(this)"><span style="color:#bbb">—</span></div></td>
    <td><div class="editable" contenteditable="true" data-field="problem_summary" data-orig="" data-placeholder="Суть обращения"></div></td>
    <td><div class="select-cell" data-field="result"  data-value="" onclick="openSelect(this)"><span style="color:#bbb">—</span></div></td>
    <td><div class="editable" contenteditable="true" data-field="comment" data-orig="" data-placeholder="Добавить..."></div></td>
    <td class="col-channel"><div class="select-cell" data-field="channel" data-value="Другой" onclick="openSelect(this)">Другой</div></td>
    <td></td>
  `;
  tr.querySelectorAll('.editable').forEach(el => el.addEventListener('blur', onBlur));
  const tbody = document.getElementById('tbody');
  if (tbody.querySelector('.no-data')) tbody.innerHTML = '';
  tbody.insertBefore(tr, tbody.firstChild);
  tr.querySelector('.editable').focus();
}

initFilters();
initResizableColumns();
load();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Day-tracker — /day-tracker  (аналог /log + колонка «Ответственный отдел»)
# ---------------------------------------------------------------------------

class DayTrackerEdit(BaseModel):
    organizer:        Optional[str] = ""
    responsible:      Optional[str] = ""
    result:           Optional[str] = ""
    comment:          Optional[str] = ""
    source_type:      Optional[str] = ""
    category:         Optional[str] = ""
    subcategory:      Optional[str] = ""
    responsible_dept: Optional[str] = ""


@router.get("/api/day-tracker")
def api_day_tracker(
    date_from:   str = Query(default=None),
    date_to:     str = Query(default=None),
    operator:    str = Query(default=""),
    channel:     str = Query(default=""),
    stype:       str = Query(default=""),
    category:    str = Query(default=""),
    subcategory: str = Query(default=""),
    result:      str = Query(default=""),
    dept:        str = Query(default=""),
    page:        int = Query(default=1, ge=1),
):
    df = date_from or str(date.today() - timedelta(days=7))
    dt = date_to   or str(date.today())

    def q(s): return s.replace("'", "\\'")

    where = f"toDate(r.ts) BETWEEN '{df}' AND '{dt}'"
    if operator:
        where += f" AND d.operator_name = '{q(operator)}'"
    if channel == "Чат":
        where += " AND d.source = 'jivo'"
    elif channel == "ЛС":
        where += " AND d.source = 'site_pm'"
    if stype:
        where += f" AND if(e.source_type!='', e.source_type, ifNull(a.source_type,'')) = '{q(stype)}'"
    if category:
        where += f" AND if(e.category!='', e.category, ifNull(a.category,'')) = '{q(category)}'"
    if subcategory:
        where += f" AND if(e.subcategory!='', e.subcategory, ifNull(a.subcategory,'')) = '{q(subcategory)}'"
    if result:
        where += f" AND if(e.result!='' AND e.result IS NOT NULL, e.result, ifNull(a.resolution_status,'')) = '{q(result)}'"
    if dept:
        where += f" AND ifNull(t.responsible_dept, '') = '{q(dept)}'"

    offset = (page - 1) * PER_PAGE

    total_rows = ch_query(f"""
        SELECT count() AS total
        FROM dialogs d
        JOIN (
            SELECT chat_id, max(received_at) AS ts
            FROM raw_dialogs
            WHERE event_name = 'chat_finished'
            GROUP BY chat_id
        ) r ON d.chat_id = r.chat_id
        LEFT JOIN (SELECT * FROM support_log_edits FINAL) e ON d.chat_id = e.chat_id
        LEFT JOIN dialog_analysis a ON d.chat_id = a.chat_id
        LEFT JOIN (SELECT * FROM day_tracker_edits FINAL) t ON d.chat_id = t.chat_id
        WHERE {where}
        FORMAT JSONEachRow
    """)
    total = int((total_rows[0] if total_rows else {}).get("total", 0))

    rows = ch_query(f"""
        SELECT
            d.chat_id                                                              AS chat_id,
            toString(toDate(r.ts))                                                 AS date,
            substring(toString(r.ts), 12, 5)                                       AS time,
            ifNull(d.operator_name, '')                                            AS operator,
            ifNull(d.visitor_name, '')                                             AS author,
            toString(ifNull(d.visitor_id, 0))                                      AS login,
            ifNull(a.contact_reason, '')                                           AS appeal_type,
            if(e.source_type  != '', e.source_type,  ifNull(a.source_type, ''))   AS source_type,
            if(e.category     != '', e.category,     ifNull(a.category, ''))      AS category,
            if(e.subcategory  != '', e.subcategory,  ifNull(a.subcategory, ''))   AS subcategory,
            ifNull(a.user_problem_summary, '')                                     AS problem_summary,
            if(e.result IS NOT NULL AND e.result != '',
               e.result, ifNull(a.resolution_status, ''))                          AS result,
            ifNull(t.responsible_dept, '')                                         AS responsible_dept,
            ifNull(e.comment, '')                                                  AS comment,
            multiIf(d.source='jivo','Чат',d.source='site_pm','ЛС',d.source)       AS channel
        FROM dialogs d
        JOIN (
            SELECT chat_id, max(received_at) AS ts
            FROM raw_dialogs
            WHERE event_name = 'chat_finished'
            GROUP BY chat_id
        ) r ON d.chat_id = r.chat_id
        LEFT JOIN dialog_analysis a ON d.chat_id = a.chat_id
        LEFT JOIN (SELECT * FROM support_log_edits FINAL) e ON d.chat_id = e.chat_id
        LEFT JOIN (SELECT * FROM day_tracker_edits FINAL) t ON d.chat_id = t.chat_id
        WHERE {where}
        ORDER BY r.ts DESC
        LIMIT {PER_PAGE} OFFSET {offset}
        FORMAT JSONEachRow
    """)

    operators = ch_query("""
        SELECT DISTINCT operator_name
        FROM dialogs
        WHERE operator_name != '' AND operator_name IS NOT NULL
        ORDER BY operator_name
        FORMAT JSONEachRow
    """)

    return JSONResponse({
        "rows":      rows,
        "operators": [r["operator_name"] for r in operators],
        "total":     total,
        "page":      page,
        "per_page":  PER_PAGE,
        "pages":     max(1, -(-total // PER_PAGE)),
    })


@router.post("/api/day-tracker/{chat_id}")
def api_day_tracker_edit(chat_id: int, payload: DayTrackerEdit):
    # Общие правки → support_log_edits (те же данные что и в /log)
    log_row = json.dumps({
        "chat_id":     chat_id,
        "organizer":   payload.organizer   or "",
        "responsible": payload.responsible or "",
        "result":      payload.result      or "",
        "comment":     payload.comment     or "",
        "source_type": payload.source_type or "",
        "category":    payload.category    or "",
        "subcategory": payload.subcategory or "",
    }, ensure_ascii=False)
    ch_execute(
        "INSERT INTO support_log_edits "
        "(chat_id, organizer, responsible, result, comment, source_type, category, subcategory) "
        "FORMAT JSONEachRow",
        data=log_row.encode("utf-8"),
    )

    # Ответственный отдел → day_tracker_edits
    dept_row = json.dumps({
        "chat_id":          chat_id,
        "responsible_dept": payload.responsible_dept or "",
    }, ensure_ascii=False)
    ch_execute(
        "INSERT INTO day_tracker_edits (chat_id, responsible_dept) FORMAT JSONEachRow",
        data=dept_row.encode("utf-8"),
    )

    return JSONResponse({"ok": True})


@router.get("/day-tracker", response_class=HTMLResponse)
def day_tracker_page():
    return _DAY_HTML


_DAY_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Day Tracker</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body {
  height: 100%; overflow: hidden;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 13px; background: #f0f2f5; color: #222;
  display: flex; flex-direction: column;
}

/* ── Toolbar ── */
.toolbar {
  flex-shrink: 0;
  background: #fff; border-bottom: 1px solid #ddd;
  padding: 8px 14px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
}
.toolbar h1 { font-size: 15px; font-weight: 600; color: #333; margin-right: 6px; }
.filter-group { display: flex; align-items: center; gap: 4px; }
.filter-group label { font-size: 12px; color: #666; white-space: nowrap; }
.toolbar input[type=date], .toolbar select {
  border: 1px solid #ccc; border-radius: 5px; padding: 4px 7px;
  font-size: 12px; background: #fafafa; color: #333;
  height: 28px;
}
.toolbar input[type=date]:focus, .toolbar select:focus {
  outline: none; border-color: #1a73e8; background: #fff;
}
.btn {
  background: #1a73e8; color: #fff; border: none; border-radius: 5px;
  padding: 0 14px; height: 28px; cursor: pointer; font-size: 13px; font-weight: 500;
  white-space: nowrap;
}
.btn:hover { background: #1557b0; }
.count { margin-left: auto; font-size: 12px; color: #888; white-space: nowrap; }

/* ── Table wrapper ── */
.wrap {
  flex: 1;
  overflow: auto;
  padding: 0 0 10px 0;
}

table {
  border-collapse: collapse;
  width: max-content; min-width: 100%;
  background: #fff;
  box-shadow: 0 1px 4px rgba(0,0,0,.08);
}

thead th {
  background: #f1f3f4;
  border-bottom: 2px solid #ddd;
  border-right: 1px solid #e0e0e0;
  padding: 8px 10px;
  text-align: left;
  font-weight: 600;
  font-size: 12px;
  color: #444;
  white-space: nowrap;
  position: sticky;
  top: 0;
  z-index: 10;
}
thead th:last-child { border-right: none; }

/* ── Ответственный отдел — выделенная колонка ── */
thead th.col-dept { background: #eef2ff; color: #3730a3; }
tbody td.col-dept { background: #f8f9ff; }

tbody tr { border-bottom: 1px solid #f0f0f0; }
tbody tr:last-child { border-bottom: none; }
tbody tr:hover { background: #f8fbff; }
tbody tr:hover td.col-dept { background: #eef2ff; }

tbody td {
  padding: 6px 10px;
  border-right: 1px solid #f0f0f0;
  vertical-align: top;
}
tbody td:last-child { border-right: none; }

.col-date    { white-space: nowrap; }
.col-time    { white-space: nowrap; color: #888; }
.col-channel { white-space: nowrap; font-weight: 500; }
.col-author  { max-width: 90px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.col-summary { max-width: 320px; word-wrap: break-word; line-height: 1.4; }
.col-login   { color: #888; font-size: 12px; }

/* ── Ресайз столбцов ── */
.col-resize-handle {
  position: absolute; right: 0; top: 0;
  width: 5px; height: 100%;
  cursor: col-resize;
  user-select: none;
  z-index: 1;
}
.col-resize-handle:hover,
.col-resize-handle.dragging { background: #1a73e8; }

/* Channel colors */
.ch-chat { color: #1a73e8; }
.ch-ls   { color: #0f9d58; }

/* Editable cells */
.editable {
  display: block;
  min-width: 80px; min-height: 20px;
  padding: 3px 5px; border-radius: 4px;
  outline: none; cursor: text;
  white-space: pre-wrap; word-break: break-word; overflow-wrap: break-word;
  transition: background .15s, box-shadow .15s;
}
.editable:empty::before {
  content: attr(data-placeholder);
  color: #bbb; font-style: italic; pointer-events: none;
}
.editable:empty { cursor: pointer; }
.editable:focus { background: #fffbea; box-shadow: 0 0 0 2px #f59e0b55; }
.editable.saving { opacity: .6; }
.editable.saved  { animation: flash 1s ease forwards; }

.select-cell {
  cursor: pointer; padding: 3px 5px; border-radius: 4px;
  min-width: 60px; min-height: 20px;
  transition: background .15s;
}
.select-cell:hover { background: #f0f7ff; }
.select-cell.saved { animation: flash 1s ease forwards; }
.select-cell select {
  width: 100%; font-size: 12px; border: 1px solid #fbbf24;
  border-radius: 3px; padding: 2px 4px; background: #fffbea;
  cursor: pointer;
}

/* Ответственный отдел — своя подсветка выпадашки */
.select-dept select { border-color: #818cf8; background: #eef2ff; }

@keyframes flash {
  0%   { background: #d1fae5; }
  100% { background: transparent; }
}

/* Result badge */
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 11px; font-weight: 600; white-space: nowrap;
}
.badge-решено     { background: #d1fae5; color: #065f46; }
.badge-не-решено  { background: #fee2e2; color: #991b1b; }
.badge-частично   { background: #fef3c7; color: #92400e; }
.badge-эскалация  { background: #ede9fe; color: #5b21b6; }
.badge-other      { background: #f3f4f6; color: #374151; }

/* Dept badge */
.dept-badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 11px; font-weight: 600; white-space: nowrap;
  background: #e0e7ff; color: #3730a3;
}

/* Problem summary truncation */
.summary-text {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden; cursor: pointer;
}
.summary-text.expanded {
  display: block; -webkit-line-clamp: unset;
}

.no-data { text-align: center; padding: 60px 20px; color: #aaa; font-size: 14px; }
.spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #ddd; border-top-color: #1a73e8; border-radius: 50%; animation: spin .6s linear infinite; vertical-align: middle; margin-right: 6px; }
@keyframes spin { to { transform: rotate(360deg); } }

/* ── Пагинация ── */
.pagination {
  flex-shrink: 0;
  display: flex; align-items: center; justify-content: center; gap: 4px;
  padding: 8px; background: #fff; border-top: 1px solid #e8e8e8;
}
.pg-btn {
  min-width: 32px; height: 28px; padding: 0 8px;
  border: 1px solid #ddd; border-radius: 4px; background: #fff;
  font-size: 12px; cursor: pointer; color: #333;
  display: flex; align-items: center; justify-content: center;
}
.pg-btn:hover:not(:disabled) { background: #f0f7ff; border-color: #1a73e8; color: #1a73e8; }
.pg-btn.active { background: #1a73e8; color: #fff; border-color: #1a73e8; font-weight: 600; }
.pg-btn:disabled { opacity: .4; cursor: default; }
.pg-info { font-size: 12px; color: #888; padding: 0 8px; }
</style>
</head>
<body>

<div class="toolbar">
  <h1>📊 Day Tracker</h1>

  <div class="filter-group">
    <label>С</label>
    <input type="date" id="date_from">
  </div>
  <div class="filter-group">
    <label>По</label>
    <input type="date" id="date_to">
  </div>
  <div class="filter-group">
    <label>Оператор</label>
    <select id="filter_operator"><option value="">Все</option></select>
  </div>
  <div class="filter-group">
    <label>Канал</label>
    <select id="filter_channel">
      <option value="">Все</option>
      <option value="Чат">Чат (Jivo)</option>
      <option value="ЛС">ЛС (сайт)</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Тип автора</label>
    <select id="filter_stype">
      <option value="">Все</option>
      <option value="Клиент">Клиент</option>
      <option value="ПВЗ">ПВЗ</option>
      <option value="Сотрудник">Сотрудник</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Категория</label>
    <select id="filter_category" onchange="updateSubcatFilter()">
      <option value="">Все</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Подкатегория</label>
    <select id="filter_subcategory"><option value="">Все</option></select>
  </div>
  <div class="filter-group">
    <label>Результат</label>
    <select id="filter_result">
      <option value="">Все</option>
      <option value="Решено">Решено</option>
      <option value="Не решено">Не решено</option>
      <option value="Частично">Частично</option>
      <option value="Эскалация">Эскалация</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Отдел</label>
    <select id="filter_dept">
      <option value="">Все</option>
    </select>
  </div>
  <button class="btn" onclick="load()">Применить</button>
  <span class="count" id="count"></span>
</div>

<div class="wrap">
  <table>
    <thead>
      <tr>
        <th>Дата</th>
        <th>Время</th>
        <th>Оператор</th>
        <th>Тип автора</th>
        <th>Автор</th>
        <th>Логин</th>
        <th>Тип</th>
        <th>Категория</th>
        <th>Подкатегория</th>
        <th>Причина обращения</th>
        <th>Результат</th>
        <th class="col-dept">Отв. отдел</th>
        <th>Комментарий</th>
        <th>Канал</th>
      </tr>
    </thead>
    <tbody id="tbody">
      <tr><td colspan="14" class="no-data"><span class="spinner"></span>Загрузка...</td></tr>
    </tbody>
  </table>
</div>

<div class="pagination" id="pagination"></div>

<script>
const today = new Date();
const week  = new Date(today); week.setDate(today.getDate() - 7);
document.getElementById('date_from').value = fmt(week);
document.getElementById('date_to').value   = fmt(today);

let currentPage = 1;

function fmt(d) { return d.toISOString().split('T')[0]; }

function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function badgeClass(result) {
  const r = (result || '').toLowerCase();
  if (r === 'решено')    return 'badge-решено';
  if (r === 'не решено') return 'badge-не-решено';
  if (r === 'частично')  return 'badge-частично';
  if (r === 'эскалация') return 'badge-эскалация';
  return r ? 'badge-other' : '';
}

// ── Константы категорий ────────────────────────────────────────────────────
const SOURCE_TYPES = ['Клиент', 'ПВЗ', 'Сотрудник'];

const DEPTS = [
  'Логистика',
  'Финансы',
  'Технический отдел',
  'Клиентский сервис',
  'Операции / ПВЗ',
  'Контент',
  'Маркетинг',
];

const CAT_MAP = {
  'Заказ и доставка':      ['Статус заказа / задержка','Заказ не найден / потерян','Статус не обновился','Объединение / формирование посылки','Возврат товара (логистика)'],
  'ПВЗ и выдача':          ['График / адрес ПВЗ','Заказ не выдается','Проблема при приеме товара','Не сканируется / нет стикера','Ручные операции / обход процесса','Стать раздающим'],
  'Оплата и финансы':      ['Возврат денег','Непонятные списания / долг','Платное хранение','Оргсбор — размер и расчёт'],
  'Технические проблемы':  ['Ошибка / сбой сайта','Некорректное отображение','Поиск / каталог','Мобильное приложение'],
  'Аккаунт и профиль':     ['Нет доступа / ошибка входа','Смена телефона / почты','Блокировка / ограничения','Управление рассылками'],
  'Качество товара':       ['Брак / дефект','Несоответствие описанию или фото','Пересорт / пришло не то','Повреждение при доставке','Нарушения — маркировка, сроки годности'],
  'Закупки и организаторы':['Вопрос по закупке','Статус закупки / оплаты','Жалоба на организатора'],
  'Пристрой':              ['Как работает пристрой','Проблемы / передача пристроя'],
  'Пожелания и инсайты':   ['Запрос нового бренда / поставщика','Запрос открытия ПВЗ в регионе','Запрос новой функции платформы','Коммерческое предложение / партнёрство','Сравнение с конкурентами (WB, Ozon)'],
  'Благодарность':         ['Благодарность'],
  'Не определено':         ['Служебное / внутреннее'],
};

function initFilters() {
  const sel = document.getElementById('filter_category');
  sel.innerHTML = '<option value="">Все</option>' +
    Object.keys(CAT_MAP).map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('');

  const dsel = document.getElementById('filter_dept');
  dsel.innerHTML = '<option value="">Все</option>' +
    DEPTS.map(d => `<option value="${esc(d)}">${esc(d)}</option>`).join('');
}

function updateSubcatFilter() {
  const cat = document.getElementById('filter_category').value;
  const sel = document.getElementById('filter_subcategory');
  const cur = sel.value;
  const subs = CAT_MAP[cat] || [];
  sel.innerHTML = '<option value="">Все</option>' +
    subs.map(s => `<option value="${esc(s)}"${s===cur?' selected':''}>${esc(s)}</option>`).join('');
}

function goPage(p) { currentPage = p; load(false); }

async function load(resetPage = true) {
  if (resetPage) currentPage = 1;

  const params = new URLSearchParams({
    date_from:   document.getElementById('date_from').value,
    date_to:     document.getElementById('date_to').value,
    operator:    document.getElementById('filter_operator').value,
    channel:     document.getElementById('filter_channel').value,
    stype:       document.getElementById('filter_stype').value,
    category:    document.getElementById('filter_category').value,
    subcategory: document.getElementById('filter_subcategory').value,
    result:      document.getElementById('filter_result').value,
    dept:        document.getElementById('filter_dept').value,
    page:        currentPage,
  });

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '<tr><td colspan="14" class="no-data"><span class="spinner"></span>Загрузка...</td></tr>';
  document.getElementById('count').textContent = '';
  document.getElementById('pagination').innerHTML = '';

  try {
    const resp = await fetch('/api/day-tracker?' + params);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    if (resetPage) {
      const sel = document.getElementById('filter_operator');
      const cur = sel.value;
      sel.innerHTML = '<option value="">Все</option>' +
        data.operators.map(o => `<option value="${esc(o)}"${o===cur?' selected':''}>${esc(o)}</option>`).join('');
    }

    render(data.rows);
    renderPagination(data.page, data.pages, data.total);
    document.getElementById('count').textContent =
      `${data.total} записей, стр. ${data.page} из ${data.pages}`;
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="14" class="no-data">Ошибка загрузки: ${e.message}</td></tr>`;
  }
}

function renderPagination(page, pages, total) {
  const el = document.getElementById('pagination');
  if (pages <= 1) { el.innerHTML = ''; return; }
  const btns = [];
  btns.push(`<button class="pg-btn" onclick="goPage(${page-1})" ${page===1?'disabled':''}>‹</button>`);
  const range = new Set([1, pages]);
  for (let i = Math.max(1, page-2); i <= Math.min(pages, page+2); i++) range.add(i);
  let prev = 0;
  for (const p of [...range].sort((a,b)=>a-b)) {
    if (prev && p - prev > 1) btns.push(`<span class="pg-info">…</span>`);
    btns.push(`<button class="pg-btn${p===page?' active':''}" onclick="goPage(${p})">${p}</button>`);
    prev = p;
  }
  btns.push(`<button class="pg-btn" onclick="goPage(${page+1})" ${page===pages?'disabled':''}>›</button>`);
  el.innerHTML = btns.join('');
}

function render(rows) {
  const tbody = document.getElementById('tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="14" class="no-data">Нет данных за выбранный период</td></tr>';
    return;
  }

  tbody.innerHTML = rows.map(r => {
    const chClass  = r.channel === 'Чат' ? 'ch-chat' : 'ch-ls';
    const bc       = badgeClass(r.result);
    const resultHtml = bc
      ? `<span class="badge ${bc}">${esc(r.result)}</span>`
      : (r.result ? esc(r.result) : '<span style="color:#bbb">—</span>');
    const deptHtml = r.responsible_dept
      ? `<span class="dept-badge">${esc(r.responsible_dept)}</span>`
      : '<span style="color:#bbb">—</span>';
    return `<tr data-id="${r.chat_id}">
      <td class="col-date">${esc(r.date)}</td>
      <td class="col-time">${esc(r.time)}</td>
      <td>${esc(r.operator)}</td>
      <td><div class="select-cell" data-field="source_type" data-value="${esc(r.source_type)}" onclick="openSelect(this)">${esc(r.source_type) || '<span style=color:#bbb>—</span>'}</div></td>
      <td class="col-author" title="${esc(r.author)}">${esc(r.author)}</td>
      <td class="col-login">${esc(extractLogin(r.author, r.source_type))}</td>
      <td>${esc(r.appeal_type)}</td>
      <td><div class="select-cell" data-field="category"    data-value="${esc(r.category)}"    onclick="openSelect(this)">${esc(r.category)    || '<span style=color:#bbb>—</span>'}</div></td>
      <td><div class="select-cell" data-field="subcategory" data-value="${esc(r.subcategory)}" onclick="openSelect(this)">${esc(r.subcategory) || '<span style=color:#bbb>—</span>'}</div></td>
      <td class="col-summary"><div class="summary-text" onclick="this.classList.toggle('expanded')">${esc(r.problem_summary)}</div></td>
      <td>${resultHtml}</td>
      <td class="col-dept"><div class="select-cell select-dept" data-field="responsible_dept" data-value="${esc(r.responsible_dept)}" onclick="openSelect(this)">${deptHtml}</div></td>
      <td><div class="editable" contenteditable="true" data-field="comment" data-orig="${esc(r.comment)}" data-placeholder="Добавить...">${esc(r.comment)}</div></td>
      <td class="col-channel ${chClass}">${esc(r.channel)}</td>
    </tr>`;
  }).join('');

  tbody.querySelectorAll('.editable').forEach(el => {
    el.addEventListener('blur', onBlur);
  });
}

async function onBlur(e) {
  const el   = e.target;
  const orig = el.dataset.orig;
  const val  = el.textContent.trim();
  if (val === orig) return;
  if (val === '') el.innerHTML = '';
  el.dataset.orig = val;
  el.classList.add('saving');
  await saveRow(el.closest('tr'), el);
  el.classList.remove('saving');
}

function extractLogin(author, sourceType) {
  if (!author) return '';
  if (sourceType === 'ПВЗ' || sourceType === 'Сотрудник') return author;
  const ci = author.indexOf(',');
  if (ci !== -1) return author.slice(ci + 1).trim();
  const li = author.toLowerCase().indexOf('лог');
  if (li !== -1) {
    const after = author.slice(li + 3).replace(/^[\\s:]+/, '').trim();
    if (after) return after;
  }
  const words = author.trim().split(/\\s+/);
  if (words.length) return words[words.length - 1];
  return author;
}

function collectFields(row) {
  const f = {};
  row.querySelectorAll('.editable').forEach(c => { f[c.dataset.field] = c.textContent.trim(); });
  row.querySelectorAll('.select-cell[data-field]').forEach(c => { f[c.dataset.field] = c.dataset.value || ''; });
  return f;
}

async function saveRow(row, feedbackEl) {
  const chatId = row.dataset.id;
  const fields = collectFields(row);
  try {
    const resp = await fetch('/api/day-tracker/' + chatId, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(fields),
    });
    if (resp.ok && feedbackEl) {
      feedbackEl.classList.add('saved');
      setTimeout(() => feedbackEl.classList.remove('saved'), 1200);
    }
  } catch (err) {
    console.error('Save error:', err);
  }
}

function openSelect(cell) {
  if (cell.querySelector('select')) return;
  const field   = cell.dataset.field;
  const current = cell.dataset.value || '';
  const row     = cell.closest('tr');

  let options = [];
  if (field === 'source_type') {
    options = SOURCE_TYPES;
  } else if (field === 'category') {
    options = Object.keys(CAT_MAP);
  } else if (field === 'subcategory') {
    const catCell = row.querySelector('[data-field="category"]');
    options = CAT_MAP[catCell ? catCell.dataset.value : ''] || [];
  } else if (field === 'responsible_dept') {
    options = DEPTS;
  }

  const sel = document.createElement('select');
  sel.innerHTML = '<option value="">— выбрать —</option>' +
    options.map(v => `<option value="${esc(v)}"${v === current ? ' selected' : ''}>${esc(v)}</option>`).join('');

  cell.innerHTML = '';
  cell.appendChild(sel);
  sel.focus();

  async function apply() {
    const val = sel.value;
    cell.dataset.value = val;

    if (field === 'responsible_dept') {
      cell.innerHTML = val
        ? `<span class="dept-badge">${esc(val)}</span>`
        : '<span style="color:#bbb">—</span>';
    } else {
      cell.innerHTML = val ? esc(val) : '<span style="color:#bbb">—</span>';
    }
    cell.onclick = () => openSelect(cell);

    if (field === 'category') {
      const subCell = row.querySelector('[data-field="subcategory"]');
      if (subCell) {
        subCell.dataset.value = '';
        subCell.innerHTML = '<span style="color:#bbb">—</span>';
      }
    }

    await saveRow(row, cell);
  }

  sel.addEventListener('change', apply);
  sel.addEventListener('blur',   apply);
}

// ── Ресайз столбцов ────────────────────────────────────────────────────────
const COL_WIDTHS_KEY = 'day_tracker_col_widths_v1';
// Дата, Время, Оператор, Тип автора, Автор, Логин, Тип, Категория, Подкатегория, Причина, Результат, Отв.отдел, Комментарий, Канал
const DEFAULT_COL_WIDTHS = [90, 55, 90, 90, 100, 110, 90, 120, 140, 340, 90, 120, 140, 55];

function initResizableColumns() {
  const ths = [...document.querySelectorAll('thead th')];
  ths.forEach((th, i) => {
    const handle = document.createElement('div');
    handle.className = 'col-resize-handle';
    th.appendChild(handle);
    let startX, startW;
    handle.addEventListener('mousedown', e => {
      startX = e.pageX; startW = th.offsetWidth;
      handle.classList.add('dragging');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
      const onMove = e => {
        const w = Math.max(30, startW + e.pageX - startX);
        th.style.width = th.style.minWidth = th.style.maxWidth = w + 'px';
      };
      const onUp = () => {
        handle.classList.remove('dragging');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        saveColWidths();
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
      e.preventDefault();
    });
  });
  restoreColWidths();
}

function saveColWidths() {
  const widths = [...document.querySelectorAll('thead th')].map(th => th.style.width || '');
  localStorage.setItem(COL_WIDTHS_KEY, JSON.stringify(widths));
}

function restoreColWidths() {
  try {
    const saved = JSON.parse(localStorage.getItem(COL_WIDTHS_KEY) || '[]');
    const ths = [...document.querySelectorAll('thead th')];
    ths.forEach((th, i) => {
      const w = saved[i] || (DEFAULT_COL_WIDTHS[i] ? DEFAULT_COL_WIDTHS[i] + 'px' : null);
      if (w) th.style.width = th.style.minWidth = th.style.maxWidth = w;
    });
  } catch (_) {}
}

initFilters();
initResizableColumns();
load();
</script>
</body>
</html>
"""
