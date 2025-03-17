#!/bin/zsh

# Clear the screen
clear

# Configuration
VENV_NAME=".venv"
INPUT_DIR="./uploads"
REQUIREMENTS_FILE="requirements.txt"
REQUIREMENTS_HASH_FILE=".requirements.md5"
VENV_PATH="$(pwd)/$VENV_NAME"
VENV_PYTHON="$VENV_PATH/bin/python3"
VENV_PIP="$VENV_PATH/bin/pip"

# Color Definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Print section header
print_section() {
    echo -e "\n${BLUE}▶ $1${NC}"
}

# Function to install a package
install_package() {
    package=$1
    echo -e "${YELLOW}Installing $package...${NC}"
    "$VENV_PIP" install --no-cache-dir "$package"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $package installed successfully"
    else
        echo -e "${RED}✗ Failed to install $package${NC}"
        return 1
    fi
}

# Function to check if requirements have changed
requirements_changed() {
    if [ ! -f "$REQUIREMENTS_HASH_FILE" ]; then
        return 0  # True - needs installation
    fi
    
    current_hash=$(md5sum "$REQUIREMENTS_FILE" | cut -d' ' -f1)
    stored_hash=$(cat "$REQUIREMENTS_HASH_FILE")
    
    [ "$current_hash" != "$stored_hash" ]
}

# Function to create/update virtual environment
setup_venv() {
    print_section "Setting up Python environment"
    
    # Create venv if it doesn't exist
    if [ ! -d "$VENV_NAME" ]; then
        echo -e "${YELLOW}Creating new virtual environment...${NC}"
        python3 -m venv "$VENV_NAME"
        
        if [ ! -f "$VENV_PYTHON" ]; then
            echo -e "${RED}Error: Virtual environment Python not found${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✓${NC} Virtual environment already exists"
    fi

    # Only update pip if installing requirements
    if requirements_changed; then
        echo -e "${YELLOW}Updating pip to latest version...${NC}"
        "$VENV_PIP" install --upgrade pip
        echo -e "${GREEN}✓${NC} Pip updated successfully"
    fi
}

# Function to install requirements
install_requirements() {
    print_section "Checking dependencies"
    
    if ! requirements_changed; then
        echo -e "${GREEN}✓${NC} Requirements unchanged - skipping installation"
        return 0
    fi
    
    echo -e "${YELLOW}Requirements changed - installing updates...${NC}"
    
    # Install packages individually
    packages=(
        "pandas>=2.0.0"
        "openpyxl>=3.1.0"
        "xlrd>=2.0.1"
        "colorama>=0.4.6"
        "tqdm>=4.65.0"
        "fuzzywuzzy>=0.18.0"
        "python-Levenshtein>=0.21.0"
        "PyYAML>=6.0.1"
    )
    
    for package in "${packages[@]}"; do
        install_package "$package" || exit 1
    done
    
    # Store new hash after successful installation
    md5sum "$REQUIREMENTS_FILE" | cut -d' ' -f1 > "$REQUIREMENTS_HASH_FILE"
    
    # Verify installations
    echo -e "\n${YELLOW}Verifying installations...${NC}"
    "$VENV_PIP" freeze
    
    echo -e "${GREEN}✓${NC} All dependencies installed successfully"
}

# Main execution
print_section "Initializing Environment"

# Setup virtual environment and install requirements
setup_venv
install_requirements

# Create uploads directory if it doesn't exist
if [ ! -d "$INPUT_DIR" ]; then
    mkdir -p "$INPUT_DIR"
    echo -e "${GREEN}✓${NC} Created uploads directory"
fi

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Launch the application
print_section "Launching Application"
"$VENV_PYTHON" src/main.py "$INPUT_DIR"

exit_code=$?

if [ $exit_code -ne 0 ]; then
    echo -e "\n${RED}✗ Processing failed${NC}"
    exit $exit_code
fi
