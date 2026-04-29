"""
Базовый формат диалога — общий для всех источников.
Каждый коннектор нормализует свои данные в этот формат.
"""

from dataclasses import dataclass, field


@dataclass
class Dialog:
    source:          str    # 'jivo' | 'site_pm' | 'claim' | ...
    dialog_id:       str    # уникальный ID внутри источника
    chat_id:         int    # числовой ID (для CH)
    finished_at:     str    # ISO datetime последнего сообщения
    visitor_name:    str    # имя клиента
    visitor_id:      str    # ID клиента
    chats_count:     int    # кол-во диалогов клиента (если известно)
    operator_name:   str    # имя оператора / организатора
    page_url:        str    # ссылка на контекст (закупку, страницу)
    plain_messages:  str    # "Имя: текст\nОператор: текст\n..."
    raw_json:        str    # оригинальные данные в JSON
    widget_id:       str = ""  # доп. метаданные (тип обращения и т.п.)

    def to_payload(self) -> dict:
        """Приводит к формату, понятному ai_processor.analyze_and_save()."""
        return {
            "source":         self.source,
            "chat_id":        self.chat_id,
            "widget_id":      self.widget_id,
            "plain_messages": self.plain_messages,
            "visitor": {
                "name":        self.visitor_name,
                "number":      self.visitor_id,
                "chats_count": self.chats_count,
            },
            "agents": [{"name": self.operator_name}],
            "page":   {"url": self.page_url},
        }
