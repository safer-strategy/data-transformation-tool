#!/bin/zsh

# Configuration
VENV_NAME=".venv"
INPUT_DIR="./uploads"
REQUIREMENTS_FILE="requirements.txt"
VENV_PATH="$(pwd)/$VENV_NAME"
VENV_PYTHON="$VENV_PATH/bin/python3"
VENV_PIP="$VENV_PATH/bin/pip"

# Color Definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'

# Function to check if virtual environment is active
is_venv_active() {
    if [ -n "$VIRTUAL_ENV" ]; then
        if [ "$(basename "$VIRTUAL_ENV")" = "$VENV_NAME" ]; then
            return 0  # True, venv is active and matches our VENV_NAME
        fi
    fi
    return 1  # False, venv is not active or doesn't match
}

# Function to create virtual environment
setup_venv() {
    if [ ! -d "$VENV_NAME" ]; then
        echo -e "${BLUE}Creating virtual environment...${NC}"
        python3 -m venv "$VENV_NAME"
    fi

    if ! is_venv_active; then
        echo -e "${BLUE}Activating virtual environment...${NC}"
        # Instead of activating, we'll use full paths
        if [ ! -f "$VENV_PYTHON" ]; then
            echo -e "${RED}Error: Virtual environment Python not found${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}Virtual environment already active${NC}"
    fi
}

# Function to install/update requirements
install_requirements() {
    if [ -f "$REQUIREMENTS_FILE" ]; then
        echo -e "${BLUE}Installing/updating requirements...${NC}"
        "$VENV_PIP" install -r "$REQUIREMENTS_FILE"
    else
        echo -e "${RED}Error: $REQUIREMENTS_FILE not found${NC}"
        exit 1
    fi
}

# Ensure uploads directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${YELLOW}Creating uploads directory...${NC}"
    mkdir -p "$INPUT_DIR"
fi

# Setup virtual environment
setup_venv

# Install requirements
install_requirements

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
"$VENV_PYTHON" src/main.py "$input_file"

exit $?
