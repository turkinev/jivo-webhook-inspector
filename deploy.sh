#!/bin/bash
# Запускать от root или через sudo на VPS (Ubuntu/Debian)

set -e

APP_DIR="/opt/jivo_inspector"
SERVICE_NAME="jivo_inspector"

echo "=== Установка зависимостей ==="
apt-get update -q
apt-get install -y python3 python3-pip python3-venv

echo "=== Копирование файлов ==="
mkdir -p "$APP_DIR/logs"
cp main.py "$APP_DIR/"
cp requirements.txt "$APP_DIR/"

echo "=== Виртуальное окружение ==="
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt" -q

echo "=== Systemd сервис ==="
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=JivoChat Webhook Inspector
After=network.target

[Service]
User=root
WorkingDirectory=$APP_DIR
EnvironmentFile=-$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port 6200
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo ""
echo "=== Готово ==="
echo "Сервис запущен на порту 6200"
echo "Endpoint для JivoChat: http://$(hostname -I | awk '{print $1}'):6200/jivo/webhook"
echo "Просмотр логов: http://$(hostname -I | awk '{print $1}'):6200/jivo/logs"
echo "Системные логи: journalctl -u $SERVICE_NAME -f"
