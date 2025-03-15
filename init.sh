#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
VENV_NAME="venv"
VENV_PYTHON="$VENV_NAME/bin/python3"
VENV_PIP="$VENV_NAME/bin/pip"
REQUIREMENTS_FILE="requirements.txt"
FLASK_ENV_FILE=".env"

print_section() {
    echo -e "\n${GREEN}=== $1 ===${NC}\n"
}

setup_flask_env() {
    print_section "Setting up Flask environment"
    
    if [ ! -f "$FLASK_ENV_FILE" ]; then
        echo -e "${YELLOW}Creating Flask environment file...${NC}"
        cat > "$FLASK_ENV_FILE" << EOL
FLASK_APP=src/app.py
FLASK_ENV=development
FLASK_DEBUG=1
SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
EOL
        echo -e "${GREEN}✓${NC} Created Flask environment file"
    else
        echo -e "${GREEN}✓${NC} Flask environment file exists"
    fi
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

    echo -e "${YELLOW}Updating pip to latest version...${NC}"
    "$VENV_PIP" install --upgrade pip > /dev/null 2>&1
    echo -e "${GREEN}✓${NC} Pip updated successfully"
}

# Main execution
setup_venv
setup_flask_env

print_section "Installing dependencies"
"$VENV_PIP" install -r "$REQUIREMENTS_FILE"

print_section "Setup complete"
echo -e "${GREEN}✓${NC} Environment ready for development"
echo -e "\nTo activate the virtual environment, run:"
echo -e "${YELLOW}source $VENV_NAME/bin/activate${NC}"
