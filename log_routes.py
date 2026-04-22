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
    url = f"http://{CH_HOST}:{CH_PORT}/?{_params()}"
    body = data if data is not None else sql.encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"CH {e.code}: {e.read().decode()[:300]}")


def ensure_table():
    """Создаёт таблицу support_log_edits если её нет."""
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


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@router.get("/api/log")
def api_log(
    date_from: str = Query(default=None),
    date_to:   str = Query(default=None),
    operator:  str = Query(default=""),
    channel:   str = Query(default=""),
):
    df = date_from or str(date.today() - timedelta(days=7))
    dt = date_to   or str(date.today())

    where = f"toDate(r.ts) BETWEEN '{df}' AND '{dt}'"
    if operator:
        op_esc = operator.replace("'", "\\'")
        where += f" AND d.operator_name = '{op_esc}'"
    if channel == "Чат":
        where += " AND d.source = 'jivo'"
    elif channel == "ЛС":
        where += " AND d.source = 'site_pm'"

    rows = ch_query(f"""
        SELECT
            d.chat_id                                                              AS chat_id,
            toString(toDate(r.ts))                                                 AS date,
            substring(toString(r.ts), 12, 5)                                       AS time,
            ifNull(d.operator_name, '')                                            AS operator,
            ifNull(d.visitor_name, '')                                             AS author,
            toString(ifNull(d.visitor_id, 0))                                      AS login,
            ifNull(a.contact_reason, '')                                           AS appeal_type,
            ifNull(a.source_type, '')                                              AS source_type,
            ifNull(a.category, '')                                                 AS category,
            ifNull(a.subcategory, '')                                              AS subcategory,
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
        LIMIT 1000
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
    })


class EditPayload(BaseModel):
    organizer:   Optional[str] = ""
    responsible: Optional[str] = ""
    result:      Optional[str] = ""
    comment:     Optional[str] = ""


@router.post("/api/log/{chat_id}")
def api_edit(chat_id: int, payload: EditPayload):
    row = json.dumps({
        "chat_id":     chat_id,
        "organizer":   payload.organizer   or "",
        "responsible": payload.responsible or "",
        "result":      payload.result      or "",
        "comment":     payload.comment     or "",
    }, ensure_ascii=False)
    ch_execute(
        "INSERT INTO support_log_edits (chat_id, organizer, responsible, result, comment) FORMAT JSONEachRow",
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
  padding: 10px;
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
  word-wrap: break-word;
  transition: background .15s, box-shadow .15s;
}
.editable:empty::before {
  content: attr(data-placeholder);
  color: #bbb;
  font-style: italic;
  pointer-events: none;
}
.editable:focus {
  background: #fffbea;
  box-shadow: 0 0 0 2px #f59e0b55;
}
.editable.saving { opacity: .6; }
.editable.saved  { animation: flash 1s ease forwards; }
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
.spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #ddd; border-top-color: #1a73e8; border-radius: 50%; animation: spin .6s linear infinite; vertical-align: middle; margin-right: 6px; }
@keyframes spin { to { transform: rotate(360deg); } }
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
  <button class="btn" onclick="load()">Применить</button>
  <span class="count" id="count"></span>
</div>

<div class="wrap">
  <table>
    <thead>
      <tr>
        <th>Дата</th>
        <th>Оператор</th>
        <th>Время</th>
        <th>Автор</th>
        <th>Логин</th>
        <th>Причина обращения</th>
        <th>Тип</th>
        <th>Тип автора</th>
        <th>Категория</th>
        <th>Подкатегория</th>
        <th>Результат</th>
        <th>Комментарий</th>
        <th>Канал</th>
      </tr>
    </thead>
    <tbody id="tbody">
      <tr><td colspan="13" class="no-data"><span class="spinner"></span>Загрузка...</td></tr>
    </tbody>
  </table>
</div>

<script>
// Defaults: last 7 days
const today = new Date();
const week  = new Date(today); week.setDate(today.getDate() - 7);
document.getElementById('date_from').value = fmt(week);
document.getElementById('date_to').value   = fmt(today);

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

async function load() {
  const params = new URLSearchParams({
    date_from: document.getElementById('date_from').value,
    date_to:   document.getElementById('date_to').value,
    operator:  document.getElementById('filter_operator').value,
    channel:   document.getElementById('filter_channel').value,
  });

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '<tr><td colspan="13" class="no-data"><span class="spinner"></span>Загрузка...</td></tr>';
  document.getElementById('count').textContent = '';

  try {
    const resp = await fetch('/api/log?' + params);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    // Обновляем список операторов
    const sel = document.getElementById('filter_operator');
    const cur = sel.value;
    sel.innerHTML = '<option value="">Все</option>' +
      data.operators.map(o => `<option value="${esc(o)}"${o===cur?' selected':''}>${esc(o)}</option>`).join('');

    render(data.rows);
    document.getElementById('count').textContent = data.rows.length + ' записей';
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="13" class="no-data">Ошибка загрузки: ${e.message}</td></tr>`;
  }
}

function render(rows) {
  const tbody = document.getElementById('tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="13" class="no-data">Нет данных за выбранный период</td></tr>';
    return;
  }

  tbody.innerHTML = rows.map(r => {
    const chClass = r.channel === 'Чат' ? 'ch-chat' : 'ch-ls';
    const bc = badgeClass(r.result);
    const resultHtml = bc
      ? `<span class="badge ${bc}">${esc(r.result)}</span>`
      : (r.result ? esc(r.result) : '<span style="color:#bbb">—</span>');
    return `<tr data-id="${r.chat_id}">
      <td class="col-date">${esc(r.date)}</td>
      <td>${esc(r.operator)}</td>
      <td class="col-time">${esc(r.time)}</td>
      <td>${esc(r.author)}</td>
      <td class="col-login">${r.login !== '0' ? esc(r.login) : ''}</td>
      <td class="col-summary"><div class="summary-text" onclick="this.classList.toggle('expanded')">${esc(r.problem_summary)}</div></td>
      <td>${esc(r.appeal_type)}</td>
      <td>${esc(r.source_type)}</td>
      <td>${esc(r.category)}</td>
      <td>${esc(r.subcategory)}</td>
      <td>${resultHtml}</td>
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
  const row  = el.closest('tr');
  const orig = el.dataset.orig;
  const val  = el.textContent.trim();

  if (val === orig) return; // не изменилось

  const chatId = row.dataset.id;
  const fields = {};
  row.querySelectorAll('.editable').forEach(c => {
    fields[c.dataset.field] = c.textContent.trim();
  });

  el.classList.add('saving');
  try {
    const resp = await fetch('/api/log/' + chatId, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(fields),
    });
    if (resp.ok) {
      el.dataset.orig = val;
      el.classList.remove('saving');
      el.classList.add('saved');
      setTimeout(() => el.classList.remove('saved'), 1200);
    }
  } catch (err) {
    el.classList.remove('saving');
    console.error('Save error:', err);
  }
}

load();
</script>
</body>
</html>
"""
