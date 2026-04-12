# Airflow ETL Practice

## Описание

Этот проект предназначен для практики ETL процессов с использованием Apache Airflow и Docker.  
В проекте поднимается локальное окружение с Apache Airflow и PostgreSQL, а также настраивается автоматическое создание пользователя. Подготовлен тестовый DAG.

## Требования

Перед началом работы необходимо установить:
- Docker Desktop
- Git

## Скачивание проекта

Склонируйте репозиторий:

```bash
git clone git@github.com:n-novi/airflow-etl-practice.git
cd airflow-etl-practice
```

## Запуск проекта

Соберите и запустите контейнеры:

```bash
docker compose up --build
```

или в фоновом режиме:

```bash
docker compose up -d --build
```

## Открытие интерфейса Airflow

После запуска откройте в браузере:

http://localhost:8080

## Данные для входа

Логин:
admin

Пароль:
admin

## Структура проекта

dags/ - DAG файлы Airflow (ETL логика)  
data/ - входные и выходные данные  
plugins/ - дополнительные плагины  
Dockerfile - сборка образа Airflow  
docker-compose.yml - настройка контейнеров  
requirements.txt - зависимости Python  

## Как работать

Чтобы добавить новый DAG:
1. Поместите файл в папку dags/
2. Airflow автоматически его подхватит
3. Активируйте DAG в интерфейсе

## Работа с данными

- входные данные находятся в data/raw
- результаты обработки сохраняются в data/processed

## Остановка проекта

```bash
docker compose down
```

## Полная очистка (включая данные)

```bash
docker compose down -v
```

## Возможные проблемы

Если Airflow не открывается сразу, подождите 30–60 секунд после запуска контейнеров.

Если порт 8080 занят, измените его в docker-compose.yml.
