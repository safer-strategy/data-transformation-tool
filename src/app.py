from flask import Flask, render_template, request, jsonify, send_file
from flask_wtf.csrf import CSRFProtect
from werkzeug.utils import secure_filename
import os
from datetime import datetime
import logging
from pathlib import Path

# Initialize Flask app
app = Flask(__name__)

# Load configuration from .env file
app.config.from_prefixed_env()

# Initialize CSRF protection
csrf = CSRFProtect(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ensure required directories exist
for directory in ['uploads', 'converts', 'schemas', 'validates', 'logs']:
    Path(directory).mkdir(exist_ok=True)

@app.route('/')
def index():
    return jsonify({
        "status": "ok",
        "message": "Data Transformation Tool API v1.1.0",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "version": "1.1.0"
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)