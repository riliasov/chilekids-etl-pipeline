# ChileKids ETL Pipeline

## Описание
ETL пайплайн для синхронизации данных из Google Sheets в Supabase (PostgreSQL).
Проект максимально упрощен и содержит только необходимую логику для экстракции, нормализации и загрузки данных.

## Структура проекта
```
chilekids-etl-pipeline/
├── src/
│   ├── config.py       # Конфигурация
│   ├── db.py           # Работа с БД и авторизация
│   ├── sheets.py       # Google Sheets (чтение/запись)
│   ├── transform.py    # Логика трансформации и загрузки
│   ├── marts.py        # Построение витрин данных
│   └── utils.py        # Утилиты (хеширование, HTTP)
├── main.py             # CLI (единая точка входа)
├── tests/              # Тесты
├── requirements.txt    # Зависимости
└── .env                # Переменные окружения
```

## Установка

### Способ 1: С Poetry (рекомендуется)

1. **Убедитесь, что Poetry установлен:**
   ```bash
   poetry --version
   # Если нет: curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Клонируйте репозиторий и установите зависимости:**
   ```bash
   git clone <repo_url>
   cd chilekids-etl-pipeline
   poetry install
   ```
   Это создаст виртуальное окружение `.venv` в папке проекта и установит все зависимости.

3. **Настроить `.env`:**
   Создайте файл `.env` на основе примера и заполните переменные:
   ```ini
   POSTGRES_URI=postgresql://user:pass@host:port/dbname
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_KEY=your-service-key
   SHEETS_SA_JSON=secrets/service_account.json
   ```

### Способ 2: С venv (legacy)

1. **Создать виртуальное окружение и установить зависимости:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Настроить `.env`:**
   Создайте файл `.env` на основе примера и заполните переменные:
   ```ini
   POSTGRES_URI=postgresql://user:pass@host:port/dbname
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_KEY=your-service-key
   SHEETS_SA_JSON=secrets/service_account.json
   ```

## Быстрый старт

### Автоматическая активация окружения (VS Code)
После установки с Poetry виртуальное окружение `.venv` будет автоматически активирован в VS Code благодаря конфигурации в `.vscode/settings.json`.

### Запуск через IDE (VS Code)
1. Откройте проект в VS Code
2. Нажмите `F5` или используйте `Run and Debug` (Cmd+Shift+D)
3. Выберите нужную конфигурацию:
   - **ELT: Run incremental process** — полный запуск ELT
   - **ELT: Run in test mode** — тестовый режим (первые 100 записей)
   - **ELT: Check environment** — проверка окружения и подключений
   - **Tests: Run all tests** — запуск всех тестов

### Запуск из терминала
```bash
# После poetry shell (активирует виртуальное окружение)
poetry shell
python main.py run --test

# Или напрямую через poetry run (без explicit shell activation)
poetry run python main.py run --test
```

## Использование

Все операции выполняются через `main.py`:

### 1. Запуск инкрементального ELT процесса
Основной режим работы. Загружает новые данные из `raw.data`, нормализует их и обновляет `staging.records`.
```bash
python main.py run
```
Опции:
- `--test`: Тестовый режим (обрабатывает только 100 записей, выводит примеры).

### 2. Загрузка данных из Google Sheets
Извлекает данные из таблицы и сохраняет их в `raw.data` (без нормализации).
```bash
python main.py load <SPREADSHEET_ID> [RANGE]
```
Пример:
```bash
python main.py load 1A2B3C... Sheet1!A:AF
```

### 3. Проверка окружения
Проверяет наличие `.env`, переменных и подключение к БД.
```bash
python main.py check
```

## Тестирование
Для запуска тестов используйте IDE конфигурацию `Tests: Run all tests` (F5 → выбрать) или напрямую:
```bash
pytest tests/ -v
```

## Разработка
- **Нормализация:** Логика парсинга полей находится в `src/transform.py`.
- **Витрины:** SQL запросы для витрин находятся в `src/marts.py`.
- **Конфиг:** Все настройки в `src/config.py`.
- **Зависимости:** Управляются через `pyproject.toml` (Poetry). Production-зависимости в `[tool.poetry.dependencies]`, dev-зависимости (pytest, etc.) в `[tool.poetry.group.dev.dependencies]`.
- **Быстрый скрипт:** Для локального быстрого запуска используйте `./run.sh` (требует `chmod +x run.sh`).


```
