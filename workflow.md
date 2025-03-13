Below is a series of actionable steps derived from the technical specification you provided. These steps guide you through building the Python system for processing CSV and XLSX files, ensuring it meets your requirements for modularity, usability by non-developers, and compliance with schema rules. Each step is practical, with specific tasks to complete the system incrementally.
Actionable Steps to Build the Python System
Step 1: Set Up the Project Environment
Goal: Establish a working Python environment and project structure.
Tasks:
Create a new Git repository (e.g., iga-data-converter) and initialize it locally:
bash
mkdir iga-data-converter
cd iga-data-converter
git init
Set up a virtual environment:
bash
python3 -m venv venv
source venv/bin/activate
Create a requirements.txt file with necessary dependencies:
pandas
openpyxl
fuzzywuzzy
inquirer
python-Levenshtein  # Optional, speeds up fuzzywuzzy
Install dependencies:
bash
pip install -r requirements.txt
Create a basic project structure:
iga-data-converter/
├── src/
│   ├── __init__.py
│   ├── reader.py
│   ├── header_mapper.py
│   ├── data_transformer.py
│   ├── validator.py
│   ├── output_generator.py
│   └── main.py
├── schema.json
├── init.sh
├── README.md
├── examples/
│   ├── input.xlsx
│   └── output.xlsx
└── requirements.txt
Step 2: Define the Target Schema
Goal: Create a schema that defines fields, synonyms, and rules for all tabs.
Tasks:
Create schema.json in the root directory with the target schema structure:
json
{
  "Users": {
    "user_id": {"type": "string", "synonyms": ["user_id", "userid", "uid"], "mandatory": true},
    "username": {"type": "string", "synonyms": ["username", "user name", "login"], "mandatory": false},
    "email": {"type": "string", "synonyms": ["email", "mail"], "mandatory": false},
    "first_name": {"type": "string", "synonyms": ["first_name", "fname"], "mandatory": false},
    "last_name": {"type": "string", "synonyms": ["last_name", "lname"], "mandatory": false},
    "full_name": {"type": "string", "synonyms": ["full_name", "name"], "mandatory": false},
    "is_active": {"type": "string", "synonyms": ["is_active", "active"], "mandatory": true, "values": ["Yes", "No"]},
    "create_at": {"type": "datetime", "synonyms": ["create_at", "created"], "mandatory": false},
    "updated_at": {"type": "datetime", "synonyms": ["updated_at", "modified"], "mandatory": false},
    "last_login_at": {"type": "datetime", "synonyms": ["last_login_at", "last_login"], "mandatory": false}
  },
  "Groups": {
    "group_id": {"type": "string", "synonyms": ["group_id", "gid"], "mandatory": true},
    "group_name": {"type": "string", "synonyms": ["group_name", "gname"], "mandatory": false}
  },
  "Roles": {
    "role_id": {"type": "string", "synonyms": ["role_id", "rid"], "mandatory": true},
    "role_name": {"type": "string", "synonyms": ["role_name", "rname"], "mandatory": false},
    "permissions": {"type": "string", "synonyms": ["permissions", "perms"], "mandatory": false}
  },
  "Resources": {
    "resource_id": {"type": "string", "synonyms": ["resource_id", "resid"], "mandatory": true},
    "resource_name": {"type": "string", "synonyms": ["resource_name", "resname"], "mandatory": false}
  },
  "User Groups": {
    "user_id": {"type": "string", "synonyms": ["user_id", "uid"], "mandatory": true},
    "group_id": {"type": "string", "synonyms": ["group_id", "gid"], "mandatory": true}
  }
  // Add remaining relationship tabs: User Roles, Group Roles, User Resources, Role Resources
}
Add comments or a separate documentation file explaining rules (e.g., at least one of user_id, username, or email is mandatory for Users; dates must be ISO 8601).
Step 3: Implement the Reader Class
Goal: Build a module to read input files into pandas DataFrames.
Tasks:
Create src/reader.py:
python
import pandas as pd
import os

class Reader:
    def read_files(self, file_path):
        if file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, sheet_name=None)
        elif file_path.endswith('.csv'):
            if os.path.isdir(file_path):
                data = {}
                for filename in os.listdir(file_path):
                    if filename.endswith('.csv'):
                        tab_name = os.path.splitext(filename)[0]
                        data[tab_name] = pd.read_csv(os.path.join(file_path, filename))
                return data
            else:
                return {os.path.splitext(os.path.basename(file_path))[0]: pd.read_csv(file_path)}
        else:
            raise ValueError("Unsupported file type. Use .xlsx or .csv")
Test with a sample XLSX file (multiple tabs) and a directory of CSVs to ensure proper reading.
Step 4: Implement the HeaderMapper Class
Goal: Map incoming headers to the schema with fuzzy matching and user review.
Tasks:
Create src/header_mapper.py:
python
from fuzzywuzzy import process
import json
import inquirer

class HeaderMapper:
    def __init__(self, schema_file):
        with open(schema_file, 'r') as f:
            self.schema = json.load(f)

    def map_headers(self, input_headers, tab_name):
        mappings = {}
        tab_schema = self.schema.get(tab_name, {})
        for header in input_headers:
            for field, details in tab_schema.items():
                if header.lower() in [s.lower() for s in details.get("synonyms", [])] or header.lower() == field.lower():
                    mappings[header] = field
                    break
            else:
                choices = list(tab_schema.keys())
                best_match = process.extractOne(header, choices)
                if best_match[1] >= 80:
                    mappings[header] = best_match[0]
                else:
                    mappings[header] = None  # Flag for review
        return mappings

    def review_mappings(self, mappings, input_headers):
        for header in input_headers:
            if mappings[header] is None:
                question = [
                    inquirer.List('field',
                                message=f"Map '{header}' to which field?",
                                choices=list(self.schema.values())[0].keys() + ['Skip'])
                ]
                answer = inquirer.prompt(question)
                mappings[header] = answer['field'] if answer['field'] != 'Skip' else None
        return mappings
Test with sample headers (e.g., ["UserID", "Name", "LastLogin"]) against the "Users" schema to verify exact and fuzzy matching.
Step 5: Implement the DataTransformer Class
Goal: Transform data based on mappings and resolve relationship identifiers.
Tasks:
Create src/data_transformer.py:
python
import pandas as pd

class DataTransformer:
    def transform_data(self, input_data, mappings):
        transformed = {}
        for tab_name, df in input_data.items():
            mapped_df = df.rename(columns=mappings.get(tab_name, {}))
            if tab_name == "Users":
                for col in ['create_at', 'updated_at', 'last_login_at']:
                    if col in mapped_df:
                        mapped_df[col] = pd.to_datetime(mapped_df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                if 'is_active' in mapped_df:
                    mapped_df['is_active'] = mapped_df['is_active'].map({True: 'Yes', False: 'No', 'True': 'Yes', 'False': 'No'})
            transformed[tab_name] = mapped_df
        return transformed

    def resolve_relationships(self, entity_data, relationship_data):
        lookups = {}
        for tab in ['Users', 'Groups', 'Roles', 'Resources']:
            if tab in entity_data:
                lookups[tab] = {}
                for key in ['user_id', 'username', 'email', 'group_id', 'role_id', 'resource_id']:
                    if key in entity_data[tab]:
                        lookups[tab][key] = {row[key]: row for _, row in entity_data[tab].iterrows() if pd.notnull(row[key])}
        transformed = {}
        for tab_name, df in relationship_data.items():
            if tab_name == "User Groups":
                df['user_id'] = df['user_id'].apply(lambda x: lookups['Users'].get('user_id', {}).get(x, {}).get('user_id', x))
                df['group_id'] = df['group_id'].apply(lambda x: lookups['Groups'].get('group_id', {}).get(x, {}).get('group_id', x))
            transformed[tab_name] = df
        return transformed
Test date conversion with sample data (e.g., "01/01/2023" → "2023-01-01T00:00:00Z") and relationship resolution with entity lookups.
Step 6: Implement the Validator Class
Goal: Ensure transformed data meets schema rules.
Tasks:
Create src/validator.py:
python
import pandas as pd

class Validator:
    def __init__(self, schema):
        self.schema = schema

    def validate_data(self, data):
        for tab_name, df in data.items():
            if tab_name == "Users":
                if not df[['user_id', 'username', 'email']].notnull().any(axis=1).all():
                    raise ValueError("Users tab missing required identifier (user_id, username, or email)")
                if 'is_active' in df and not df['is_active'].isin(['Yes', 'No']).all():
                    raise ValueError("Users tab is_active must be 'Yes' or 'No'")
            elif tab_name in ["User Groups", "User Roles", "Group Roles", "User Resources", "Role Resources"]:
                if df.isnull().any().any():
                    raise ValueError(f"{tab_name} tab has missing values")
Test with sample data missing mandatory fields or with invalid is_active values.
Step 7: Implement the OutputGenerator Class
Goal: Generate the final Excel file with proper structure.
Tasks:
Create src/output_generator.py:
python
import pandas as pd

class OutputGenerator:
    def generate_excel(self, data, output_path):
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for tab_name, df in data.items():
                df.to_excel(writer, sheet_name=tab_name, index=False)
Test by writing sample transformed data to an Excel file and verifying the output structure.
Step 8: Create the Main Script and User Interface
Goal: Tie all components together with a user-friendly CLI.
Tasks:
Create src/main.py:
python
from reader import Reader
from header_mapper import HeaderMapper
from data_transformer import DataTransformer
from validator import Validator
from output_generator import OutputGenerator
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <input_file_or_directory>")
        sys.exit(1)
    input_path = sys.argv[1]
    reader = Reader()
    mapper = HeaderMapper('../schema.json')
    transformer = DataTransformer()
    validator = Validator(mapper.schema)
    generator = OutputGenerator()

    # Read input
    data = reader.read_files(input_path)
    
    # Map headers
    mappings = {}
    for tab_name, df in data.items():
        mappings[tab_name] = mapper.map_headers(df.columns, tab_name)
        mappings[tab_name] = mapper.review_mappings(mappings[tab_name], df.columns)
    
    # Transform data
    transformed_data = transformer.transform_data(data, mappings)
    entity_data = {k: v for k, v in transformed_data.items() if k in ["Users", "Groups", "Roles", "Resources"]}
    rel_data = {k: v for k, v in transformed_data.items() if k not in entity_data}
    transformed_rel = transformer.resolve_relationships(entity_data, rel_data)
    transformed_data.update(transformed_rel)
    
    # Validate
    validator.validate_data(transformed_data)
    
    # Generate output
    generator.generate_excel(transformed_data, "output.xlsx")
    print("Output generated as output.xlsx")

if __name__ == "__main__":
    main()
Test running python src/main.py examples/input.xlsx and verify the process from input to output.
Step 9: Finalize Setup and Documentation
Goal: Ensure non-developers can set up and use the system easily.
Tasks:
Create init.sh:
bash
#!/bin/bash
if ! command -v python3 &> /dev/null; then
    echo "Installing Python 3..."
    brew install python3
fi
pip3 install -r requirements.txt
Make it executable: chmod +x init.sh.
Write README.md:
# IGA Data Converter
Converts CSV/XLSX identity data into a standardized Excel format.

## Setup
1. Clone the repo: `git clone <repo-url>`
2. Run `./init.sh` to install dependencies (MacBook required)
3. Place input files in `examples/` or specify a path

## Usage
Run `python src/main.py <input_file_or_directory>`
- Follow prompts to review header mappings
- Output will be saved as `output.xlsx`

## Examples
See `examples/input.xlsx` for sample input and `examples/output.xlsx` for expected output.
Add sample files to examples/:
input.xlsx with tabs (Users, Groups, etc.) and varied headers.
output.xlsx showing the expected transformed result.
Step 10: Test and Refine
Goal: Ensure the system works end-to-end and handles edge cases.
Tasks:
Test with:
A complete XLSX file with all tabs.
A directory of CSVs with missing tabs.
Data with unmapped headers requiring review.
Invalid data (e.g., missing mandatory fields).
Refine error messages and user prompts based on test results.
Commit all changes to Git and push to your repository.
Notes
Time Estimate: Each step might take 1-3 hours depending on familiarity with Python and testing needs, totaling ~15-20 hours for initial implementation.
Flexibility: Add logging (e.g., import logging) for better feedback if needed later.
Next Steps: After basic functionality, consider caching mappings or adding a GUI if non-developer feedback suggests it’s necessary.
These steps provide a clear path to build, test, and deploy your system, aligning with the original specification while making it actionable for development. Let me know if you'd like to adjust or expand any step!