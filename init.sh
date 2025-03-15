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

# Check if script is being sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    IS_SOURCED=0
else
    IS_SOURCED=1
fi

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

setup_venv() {
    print_section "Setting up Python environment"
    
    # Check if we're already in a virtual environment
    if [[ "$VIRTUAL_ENV" != "" ]]; then
        echo -e "${GREEN}✓${NC} Already in virtual environment"
        return
    fi
    
    # Only create venv if it doesn't exist
    if [ ! -d "$VENV_NAME" ]; then
        echo -e "${YELLOW}Creating virtual environment...${NC}"
        python3 -m venv "$VENV_NAME"
        
        if [ ! -f "$VENV_PYTHON" ]; then
            echo -e "${RED}Error: Virtual environment Python not found${NC}"
            exit 1
        fi
        
        # Only update pip on fresh install
        echo -e "${YELLOW}Updating pip to latest version...${NC}"
        "$VENV_PIP" install --upgrade pip > /dev/null 2>&1
        echo -e "${GREEN}✓${NC} Pip updated successfully"
        
        # Install requirements only on fresh install
        echo -e "${YELLOW}Installing dependencies...${NC}"
        "$VENV_PIP" install -r "$REQUIREMENTS_FILE"
        echo -e "${GREEN}✓${NC} Dependencies installed"
    else
        # Check if requirements have changed
        if [ -f "$REQUIREMENTS_FILE.md5" ]; then
            old_md5=$(cat "$REQUIREMENTS_FILE.md5")
            new_md5=$(md5sum "$REQUIREMENTS_FILE" | cut -d' ' -f1)
            if [ "$old_md5" != "$new_md5" ]; then
                echo -e "${YELLOW}Requirements changed, updating dependencies...${NC}"
                "$VENV_PIP" install -r "$REQUIREMENTS_FILE"
                echo "$new_md5" > "$REQUIREMENTS_FILE.md5"
                echo -e "${GREEN}✓${NC} Dependencies updated"
            else
                echo -e "${GREEN}✓${NC} Dependencies up to date"
            fi
        else
            # First time checking requirements
            md5sum "$REQUIREMENTS_FILE" | cut -d' ' -f1 > "$REQUIREMENTS_FILE.md5"
            echo -e "${GREEN}✓${NC} Dependencies up to date"
        fi
    fi
}

setup_directories() {
    print_section "Creating required directories"
    
    for dir in "uploads" "converts" "schemas" "validates" "logs"; do
        if [ ! -d "$dir" ]; then
            echo -e "${YELLOW}Creating $dir directory...${NC}"
            mkdir -p "$dir"
            echo -e "${GREEN}✓${NC} Created $dir directory"
        else
            echo -e "${GREEN}✓${NC} $dir directory exists"
        fi
    done
}

start_app() {
    # Only start if not already running
    if ! pgrep -f "python src/app.py" > /dev/null; then
        print_section "Starting application"
        echo -e "${YELLOW}Starting Flask application...${NC}"
        python src/app.py
    else
        echo -e "${GREEN}✓${NC} Application already running"
    fi
}

# Main execution
setup_venv
setup_flask_env
setup_directories

print_section "Setup complete"
echo -e "${GREEN}✓${NC} Environment ready"

# Source the virtual environment if not already in one
if [[ "$VIRTUAL_ENV" == "" ]]; then
    source "$VENV_NAME/bin/activate"
fi

# Start the application only if script is not being sourced
if [ $IS_SOURCED -eq 0 ]; then
    start_app
fi
