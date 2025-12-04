# ChileKids ETL Pipeline — Google Sheets to Supabase

Async ETL pipeline для извлечения данных из Google Sheets и загрузки в Supabase PostgreSQL с нормализацией и созданием data marts.

## Архитектура

- **Extract** (`src/extract/`): Асинхронное извлечение из Google Sheets
- **Transform** (`src/transform/`): Нормализация данных с обработкой дат, чисел, валют
- **Load** (`src/load/`): Массовая загрузка в `staging.records` с использованием `executemany`
- **Marts** (`src/marts/`): Агрегированные витрины данных

## Быстрый старт

### 1. Настройка окружения

Создайте файл `.env`:
```bash
POSTGRES_URI=postgresql://user:pass@host:5432/db
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your_service_key
SHEETS_SA_JSON=./secrets/service-account.json
```

### 2. Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Запуск

```bash
python main.py              # Полный ETL
python main.py --test       # Тестовый режим (лимит 100 записей)
```

## Скрипты

- `scripts/load_sheet_to_raw.py` — Загрузка из Sheets в raw.data
- `scripts/check_env.py` — Проверка подключения к БД

## Тесты

```bash
.venv/bin/python -m pytest tests/ -v
```

## CI/CD

GitHub Actions запускается ежедневно в 03:00 UTC. Настройте секреты:
- `POSTGRES_URI`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `SHEETS_SA_JSON`
```
