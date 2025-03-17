#!/bin/zsh

# Clear the screen
clear

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

# Print section header
print_section() {
    echo -e "\n${BLUE}▶ $1${NC}"
}

# Function to check if virtual environment is active
is_venv_active() {
    if [ -n "$VIRTUAL_ENV" ]; then
        if [ "$(basename "$VIRTUAL_ENV")" = "$VENV_NAME" ]; then
            return 0
        fi
    fi
    return 1
}

# Function to check if requirements need updating
requirements_need_update() {
    if [ ! -f "$VENV_PATH/requirements.md5" ]; then
        return 0
    fi
    current_md5=$(md5sum "$REQUIREMENTS_FILE" | cut -d' ' -f1)
    stored_md5=$(cat "$VENV_PATH/requirements.md5")
    if [ "$current_md5" != "$stored_md5" ]; then
        return 0
    fi
    return 1
}

# Function to create/update virtual environment
setup_venv() {
    print_section "Setting up Python environment"
    
    if [ ! -d "$VENV_NAME" ]; then
        echo -e "${YELLOW}Creating virtual environment...${NC}"
        python3 -m venv "$VENV_NAME"
        
        if [ ! -f "$VENV_PYTHON" ]; then
            echo -e "${RED}Error: Virtual environment Python not found${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✓${NC} Virtual environment exists"
    fi

    # Update pip to latest version (suppress output but keep errors visible)
    echo -e "${YELLOW}Updating pip to latest version...${NC}"
    "$VENV_PIP" install --upgrade pip > /dev/null 2>&1
    echo -e "${GREEN}✓${NC} Pip updated successfully"
}

# Function to install/update requirements
install_requirements() {
    print_section "Checking dependencies"
    
    if [ -f "$REQUIREMENTS_FILE" ]; then
        if requirements_need_update; then
            echo -e "${YELLOW}Installing/updating requirements...${NC}"
            # Suppress 'Requirement already satisfied' messages
            "$VENV_PIP" install -r "$REQUIREMENTS_FILE" 2>&1 | grep -v "Requirement already satisfied" || true
            md5sum "$REQUIREMENTS_FILE" | cut -d' ' -f1 > "$VENV_PATH/requirements.md5"
            echo -e "${GREEN}✓${NC} Requirements updated successfully"
        else
            echo -e "${GREEN}✓${NC} Requirements are up to date"
        fi
    else
        echo -e "${RED}✗ Error: $REQUIREMENTS_FILE not found${NC}"
        exit 1
    fi
}

# Main execution starts here
# Setup virtual environment and install requirements
setup_venv
install_requirements

# Launch the AMT-8000 CLI
"$VENV_PYTHON" src/main.py "$INPUT_DIR"

exit_code=$?

if [ $exit_code -ne 0 ]; then
    echo -e "\n${RED}✗ Processing failed${NC}"
    exit $exit_code
fi
