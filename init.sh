#!/bin/zsh

# Configuration
VENV_NAME=".venv"
INPUT_DIR="./uploads"
REQUIREMENTS_FILE="requirements.txt"

# Color Definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'

# Simple menu
echo -e "\n${CYAN}Processing file from uploads directory...${NC}"

if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${RED}Error: uploads directory not found${NC}"
    exit 1
fi

# Get all Excel and CSV files
files=($INPUT_DIR/*.xlsx $INPUT_DIR/*.csv(N))

if [ ${#files[@]} -eq 0 ]; then
    echo -e "${RED}No Excel or CSV files found in uploads directory${NC}"
    exit 1
fi

# Process the first file found
input_file="${files[1]}"
echo -e "${GREEN}Processing: ${input_file}${NC}"
python src/main.py "$input_file"

exit $?
