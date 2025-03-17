# AMT-8000 Data Transformation Tool

## Overview

The AMT-8000 is a retro sci-fi themed command-line interface for transforming and validating data files. It provides an engaging, interactive experience while maintaining professional functionality.

## Quick Start

```bash
# Process files
python src/main.py uploads/

# View transmission history
python src/main.py --history

# Delete transmissions matching pattern
python src/main.py --delete "2024-01"

# Disable color output
python src/main.py uploads/ --no-color
```

## Features

### Retro Sci-Fi Interface
- Stylized boot sequence with system checks
- Mission-oriented terminology
- Progress indicators and status messages
- Color-coded output (with --no-color option)

### Core Functionality
- Multi-format input support (Excel/CSV)
- Smart column mapping with fuzzy matching
- Data validation and transformation
- Progress tracking for long operations
- Transmission history management

### Command Options
```bash
python src/main.py [-h] [--no-color] [--page-size PAGE_SIZE] [--history] [--delete PATTERN] [input_path]

arguments:
  input_path            Input file or directory path
  --no-color           Disable color output
  --page-size PAGE_SIZE
                      Number of items per page (default: 5)
  --history           View transmission history
  --delete PATTERN    Delete transmissions matching pattern
```

### Operation Phases

1. **Boot Sequence**
   - System initialization
   - Component checks
   - Mission parameters loading

2. **Scan Phase**
   - Directory scanning
   - File detection
   - Sample data preview

3. **Alignment Phase**
   - Column mapping
   - Data validation
   - Configuration review

4. **Transmission Phase**
   - Data transformation
   - Progress tracking
   - Output generation
   - Log entry creation

## Examples

### Processing Files
```bash
# Process single file
python src/main.py uploads/data.xlsx

# Process directory
python src/main.py uploads/
```

### Managing Transmissions
```bash
# View transmission history
python src/main.py --history

# Delete transmissions from January 2024
python src/main.py --delete "2024-01"

# Delete transmissions by filename pattern
python src/main.py --delete "user_data"
```

### Display Options
```bash
# Disable color output
python src/main.py uploads/ --no-color

# Adjust page size for large datasets
python src/main.py uploads/ --page-size 10
```

## Error Handling

The AMT-8000 includes comprehensive error handling:
- Input validation
- File access checks
- Data transformation validation
- Progress monitoring
- User interruption handling
- Detailed error messages

## Best Practices

1. **File Preparation**
   - Place input files in the `uploads/` directory
   - Use consistent column naming
   - Ensure file permissions are correct

2. **Operation**
   - Review mapping suggestions carefully
   - Monitor progress indicators
   - Check transmission logs regularly
   - Use pattern-based cleanup for maintenance

3. **Troubleshooting**
   - Check error messages in transmission log
   - Use --no-color for compatibility issues
   - Adjust page size for different dataset sizes

## System Requirements

- Python 3.8+
- zsh shell (for automated setup)

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/yourusername/data-transformation-tool.git
cd data-transformation-tool
```

2. Place your input file(s) in the `uploads` directory

3. Run the initialization script:
```bash
chmod +x init.sh
./init.sh
```

The script will:
- Create a virtual environment
- Install required dependencies
- Process files from the `uploads` directory
- Generate output files in the `converts` directory

## Manual Installation (Alternative)

If you prefer manual setup:

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run manually:
```bash
python src/main.py <input_file_or_directory>
```

## Project Structure

```
├── init.sh                    # Automated setup and execution script
├── src/
│   ├── main.py               # Entry point
│   ├── reader.py             # Input file reader
│   ├── header_mapper.py      # Column mapping logic
│   ├── data_transformer.py   # Data transformation
│   ├── validator.py          # Data validation
│   ├── output_generator.py   # Output file generation
│   ├── schema.json          # Data schema definition
│   └── header_mappings.yaml # Saved header mappings
├── uploads/                  # Input directory
├── converts/                 # Output directory
├── tests/                   # Test files
├── requirements.txt         # Dependencies
└── README.md               # This file
```

## Data Flow

```mermaid
flowchart TD
    A[Input File] --> B[Reader]
    B --> C[Header Mapper]
    C --> D[Data Transformer]
    D --> E[Validator]
    E --> F{Valid?}
    F -->|Yes| G[Valid Output]
    F -->|No| H[Invalid Output]
    
    subgraph "Header Mapping Process"
    C1[Load Schema] --> C2[Fuzzy Match]
    C2 --> C3[User Review]
    C3 --> C4[Save Mappings]
    end
    
    subgraph "Transformation Process"
    D1[Map Fields] --> D2[Format Dates]
    D2 --> D3[Convert Booleans]
    D3 --> D4[Handle Relationships]
    end
    
    subgraph "Validation Process"
    E1[Check Required Fields] --> E2[Validate Formats]
    E2 --> E3[Verify Relationships]
    end
```

## Schema Rules

### Entity Data
- **Users**:
  - Required: user_id/username/email
  - Required for mapping: first_name + last_name OR full_name
    - All 3 fields (first_name, last_name, full_name) must be populated in output
  - is_active: "Yes" or "No"
  - Dates in ISO 8601 format

- **Groups**:
  - Required: group_id or group_name
  
- **Roles**:
  - Required: role_id or role_name
  
- **Resources**:
  - Required: resource_id or resource_name

### Relationships
- **User Groups**:
  - All fields required
  - user_id: Uses Users tab value, or email, or username
  - group_id: Uses Groups tab value or incremental number

- **User Roles**:
  - All fields required
  - user_id: Same rules as User Groups
  - role_id: Uses Roles tab value or role_name

- **Group Roles**:
  - All fields required
  - group_id: Uses Groups tab value or incremental number
  - role_id: Uses Roles tab value or role_name

- **User Resources**:
  - All fields required
  - user_id: Same rules as User Groups
  - resource_id: Uses Resources tab value or resource_name

- **Role Resources**:
  - All fields required
  - role_id: Uses Roles tab value or role_name
  - resource_id: Uses Resources tab value or resource_name

## Error Handling

The tool generates two output files:
1. `converted_[filename].xlsx`: Contains valid records
2. `invalid_records_[filename].xlsx`: Contains invalid records with error details

## Logging

- Log file: `validation.log`
- Includes:
  - Transformation errors
  - Validation failures
  - Processing statistics

## Interactive Features

- Smart header mapping with fuzzy matching
- Preview of data samples during mapping
- Ability to save and reuse mappings
- Interactive confirmation of mappings
- Option to skip optional fields
- Progress indicators during processing

## Best Practices

1. **Input Files**:
   - Place files in the `uploads` directory
   - Use either Excel (.xlsx) or CSV format
   - Ensure data consistency within columns

2. **Header Mapping**:
   - Review automatic mappings carefully
   - Use saved mappings for consistency
   - Map all mandatory fields

3. **Data Validation**:
   - Check invalid records output
   - Review validation.log for errors
   - Correct source data if needed

## Troubleshooting

Common issues and solutions:

1. **Environment Setup**:
   ```bash
   # If init.sh fails, try:
   chmod +x init.sh
   ./init.sh
   ```

2. **Input Files**:
   - Ensure files are not open in other applications
   - Check file permissions
   - Verify file format (Excel/CSV)

3. **Processing Errors**:
   - Check validation.log for details
   - Ensure all mandatory fields are mapped
   - Verify data formats match schema requirements

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guide
- Add tests for new features
- Update documentation
- Maintain backward compatibility

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support:
1. Check the documentation
2. Review closed issues
3. Open a new issue with:
   - Description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - Log files and screenshots

## Acknowledgments

- Thanks to all contributors
- Built with Python and open-source libraries
- Inspired by real-world data transformation needs

## Changelog

### v1.0.0 (Initial Release)
- Multi-format input support (Excel/CSV)
- Smart header mapping with fuzzy matching
- Data validation and transformation
- Standardized output generation
- Interactive CLI interface
- Comprehensive logging
