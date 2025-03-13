import pandas as pd
import sys
from datetime import datetime

class SchemaValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
        
        # Define allowed tabs based on schema rules
        self.entity_tabs = {
            'Users', 'Groups', 'Roles', 'Resources'
        }
        self.relationship_tabs = {
            'User Groups', 'User Roles', 'Group Roles',
            'User Resources', 'Role Resources', 'Group Resources'
        }
        self.allowed_tabs = self.entity_tabs | self.relationship_tabs
        
    def validate_tabs(self, df_dict):
        """Validate that all tabs in the Excel file are defined in the schema"""
        excel_tabs = set(df_dict.keys())
        unexpected_tabs = excel_tabs - self.allowed_tabs
        
        if unexpected_tabs:
            self.warnings.append(
                f"Found unexpected tabs that are not defined in the schema: {', '.join(unexpected_tabs)}"
            )
        
        return unexpected_tabs

    def validate_datetime(self, value):
        """Validate if a value matches ISO 8601 datetime format"""
        if pd.isna(value):
            return True
        try:
            datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ")
            return True
        except ValueError:
            return False

    def validate_users(self, df):
        """Validate Users tab rules"""
        if 'Users' not in df:
            return
        
        users_df = df['Users']
        for idx, row in users_df.iterrows():
            # Check identifier fields
            has_identifier = any(not pd.isna(row.get(field)) 
                               for field in ['user_id', 'username', 'email'])
            if not has_identifier:
                self.errors.append(f"Users row {idx+2}: Missing required identifier (user_id, username, or email)")

            # Validate is_active field
            is_active = row.get('is_active')
            if not pd.isna(is_active) and str(is_active) not in ['Yes', 'No']:
                self.errors.append(f"Users row {idx+2}: is_active must be 'Yes' or 'No'")

            # Validate datetime fields
            for date_field in ['created_at', 'updated_at', 'last_login_at']:
                if date_field in row and not pd.isna(row[date_field]):
                    if not self.validate_datetime(row[date_field]):
                        self.errors.append(f"Users row {idx+2}: Invalid datetime format in {date_field}")

def validate_excel_file(file_path):
    """Validate a single Excel file"""
    print(f"\nValidating file: {file_path}")
    
    try:
        # Read all sheets from the Excel file
        df_dict = pd.read_excel(file_path, sheet_name=None)
        
        validator = SchemaValidator()
        
        # Validate tabs
        unexpected_tabs = validator.validate_tabs(df_dict)
        if unexpected_tabs:
            print("\nValidation warnings found:")
            for warning in validator.warnings:
                print(f"- {warning}")
        
        # Run all validations
        validator.validate_users(df_dict)
        # Add other validation methods here as needed
        
        if validator.errors:
            print("\nValidation errors found:")
            for error in validator.errors:
                print(f"- {error}")
            return False
        else:
            print("No validation errors found.")
            return True
            
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_schema.py <excel_file>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    if validate_excel_file(file_path):
        sys.exit(0)
    else:
        sys.exit(1)
