#!/bin/bash

# Create project structure
mkdir -p iga-data-converter/src
mkdir -p iga-data-converter/examples

# Create Python files
touch iga-data-converter/src/__init__.py
touch iga-data-converter/src/reader.py
touch iga-data-converter/src/header_mapper.py
touch iga-data-converter/src/data_transformer.py
touch iga-data-converter/src/validator.py
touch iga-data-converter/src/output_generator.py
touch iga-data-converter/src/main.py

# Create other necessary files
touch iga-data-converter/schema.json
touch iga-data-converter/requirements.txt
touch iga-data-converter/README.md