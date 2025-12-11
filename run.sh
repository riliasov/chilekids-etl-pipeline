#!/usr/bin/env zsh
# ./run.sh
set -euo pipefail

# Быстрый скрипт для первого запуска проекта на macOS / Linux (zsh)
# - создает виртуальное окружение `.venv` если нужно
# - активирует окружение
# - обновляет pip и устанавливает зависимости из `requirements.txt`
# - запускает `main.py` с переданными аргументами

if [ -z "${PYTHON:-}" ]; then
  PYTHON=python3
fi

if [ ! -d ".venv" ]; then
  echo "Создаю виртуальное окружение .venv..."
  $PYTHON -m venv .venv
fi

# Активируем окружение (работает в zsh/bash)
. .venv/bin/activate

echo "Проверяю наличие Poetry..."
if command -v poetry &> /dev/null; then
  echo "Устанавливаю зависимости через Poetry..."
  poetry install
  echo "Запускаю main.py через Poetry..."
  exec poetry run python main.py "$@"
else
  echo "Poetry не найден. Используюю pip (legacy)..."
  echo "Обновляю pip и устанавливаю зависимости..."
  pip install --upgrade pip
  pip install -r requirements.txt
  
  # Установить PYTHONPATH для корректного разрешения импортов
  export PYTHONPATH="${PYTHONPATH:-}:${PWD}"
  
  echo "Запускаю main.py..."
  exec .venv/bin/python main.py "$@"
fi