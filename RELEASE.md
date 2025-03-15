# Data Transformation and Validation Tool v1.0.0

## Overview
Initial release of a powerful Python-based tool designed to streamline the processing of user access management data. This tool transforms messy Excel/CSV files into clean, standardized formats while ensuring data validity and consistency.

## Key Features
- **Multi-format Support**: Handles both Excel (.xlsx) and CSV input files
- **Smart Header Mapping**: 
  - Automatic column mapping with fuzzy matching
  - Interactive mapping confirmation
  - Mapping memory for consistent processing
- **Comprehensive Data Validation**:
  - Schema-based validation rules
  - User identification verification
  - Date format standardization
  - Relationship integrity checks
- **Data Transformation**:
  - Standardizes date/time to ISO 8601
  - Converts boolean values to Yes/No
  - Normalizes user identification fields
  - Handles relationship mappings

## Entity Support
- Users (with flexible identification options)
- Groups
- Roles
- Resources
- All associated relationships

## Technical Specifications
- Python 3.8+ compatibility
- Automated setup via init.sh
- Virtual environment management
- Comprehensive error handling
- Detailed logging system

## Installation
```bash
git clone <repository-url>
cd data-transformation-tool
./init.sh
```

## Quick Start
1. Place input files in the `uploads/` directory
2. Run: `python src/main.py <input_file_or_directory>`
3. Find processed files in the `converts/` directory

## What's Included
- `data-transformation-tool.zip`: Complete source code and documentation
- Installation scripts
- Example files
- Comprehensive documentation

## Requirements
- Python 3.8+
- zsh shell (for automated setup)
- See requirements.txt for Python dependencies

## Documentation
Refer to README.md for detailed usage instructions and best practices.

## Support
For issues and feature requests, please use the GitHub issues tracker.

## License
MIT License - See LICENSE file for details