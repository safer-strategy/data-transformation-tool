# Data Transformation Tool v1.1.0

## New Features
- Flask-based GUI interface for easier data manipulation
- Automated source detection with fuzzy matching
- Enhanced schema storage and versioning
- Real-time progress tracking
- Comprehensive error handling
- Cross-browser support (Chrome, Firefox)

## Installation
Run the setup script:
```bash
./init.sh
```
The script is idempotent and can be safely run multiple times.

## Quick Start
1. GUI Interface (New!):
   ```bash
   source venv/bin/activate
   python src/app.py
   ```
   Then open http://localhost:5000 in your browser

2. CLI Usage (Traditional):
   ```bash
   source venv/bin/activate
   python src/main.py input_file.csv
   ```

## System Requirements
- Python 3.8+
- Modern web browser (Chrome, Firefox)
- 10MB minimum free disk space

## Directory Structure
```
.
├── src/
│   ├── app.py          # Flask application (New!)
│   └── main.py         # CLI interface
├── uploads/            # File upload directory
├── converts/           # Conversion output
├── schemas/            # Schema storage
├── validates/          # Validation results
└── logs/              # Application logs
```

## New GUI Features
- Drag-and-drop file upload
- Interactive schema mapping
- Real-time validation feedback
- Progress tracking
- Error notifications
- Accessibility compliance

## Schema Management
- Version control for schemas
- Automated schema matching
- Schema validation
- Sample data extraction

## Performance
- Handles files up to 10MB
- Real-time processing feedback
- Optimized for large datasets

## Security
- Secure file handling
- Input validation
- Access control
- Error handling

## Troubleshooting
Check application logs in `logs/` directory

## Contributing
See CONTRIBUTING.md for development guidelines.

## License
[License Type] - See LICENSE file for details

## Support
For issues and feature requests, please use the GitHub issue tracker.
