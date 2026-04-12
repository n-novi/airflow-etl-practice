#!/bin/bash
set -e

echo "=== Airflow init starting ==="

# ждём postgres (очень важно)
echo "Waiting for Postgres..."
sleep 10

# инициализация базы
airflow db migrate

# создаём пользователя (если нет)
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

echo "=== Airflow init done ==="