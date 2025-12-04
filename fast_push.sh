#!/bin/bash
# ./fast_push.sh <commit_message> to push changes to GitHub

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting manual git push...${NC}"

# Check status
git status

# Add all changes
echo -e "${YELLOW}Adding all changes...${NC}"
git add .

# Ask for commit message
echo -e "${GREEN}Enter commit message:${NC}"
read commit_message

if [ -z "$commit_message" ]; then
    echo "Commit message cannot be empty. Aborting."
    exit 1
fi

# Commit
git commit -m "$commit_message"

# Push
echo -e "${YELLOW}Pushing to origin...${NC}"
git push

echo -e "${GREEN}Successfully pushed to GitHub!${NC}"
