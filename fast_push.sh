#!/bin/bash
# ./fast_push.sh <commit_message> to push changes to GitHub

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🚀 Начинаем отправку в GitHub...${NC}"

# Check status
git status

# Add all changes
echo -e "${YELLOW}📦 Добавляем все изменения...${NC}"
git add .

# Ask for commit message
echo -e "${GREEN}📝 Введите сообщение коммита:${NC}"
read commit_message

if [ -z "$commit_message" ]; then
    echo "❌ Сообщение коммита не может быть пустым. Отмена."
    exit 1
fi

# Commit
git commit -m "$commit_message"

# Push
echo -e "${YELLOW}⬆️ Отправляем на сервер...${NC}"
git push

echo -e "${GREEN}✅ Успешно отправлено в GitHub!${NC}"
