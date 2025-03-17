import pandas as pd
import logging
from typing import Dict, Tuple, Any, List

# Set logging level to DEBUG
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, schema: Dict):
        self.schema = schema
        self.logger = logging.getLogger(__name__)

    def validate_data(self, data: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        """Validate all data according to schema rules."""
        valid_data = {}
        invalid_data = {}

        for tab_name, df in data.items():
            print(f"\n{'='*50}")
            print(f"Processing {tab_name}")
            print(f"{'='*50}")
            print(f"Total records: {len(df)}")
            
            try:
                if df.empty:
                    print(f"\nTab {tab_name} is empty, skipping validation")
                    continue

                valid_mask, reasons = self._validate_tab(df, tab_name)
                
                # Display validation results
                if reasons:  # Only show if there are validation messages
                    print("\nValidation Results:\n")
                    for reason in reasons:
                        print(f"- {reason}")
                
                valid_records = df[valid_mask]
                invalid_records = df[~valid_mask]
                
                print(f"\nValidation Summary for {tab_name}:")
                print(f"- Total records: {len(df)}")
                print(f"- Valid records: {len(valid_records)}")
                print(f"- Invalid records: {len(invalid_records)}")
                
                if len(valid_records) > 0:
                    valid_data[tab_name] = valid_records
                if len(invalid_records) > 0:
                    invalid_data[tab_name] = invalid_records

            except Exception as e:
                print(f"Error validating {tab_name}: {str(e)}")
                raise

        return valid_data, invalid_data

    def _validate_users(self, df: pd.DataFrame) -> Dict:
        """Validate users tab with detailed output."""
        results = {}
        
        print("\n► ANALYZING FIELD STRUCTURE")
        print("  ═════════════════════════")
        print(f"  • DETECTED FIELDS: {', '.join(df.columns.tolist())}")
        
        # Check for user_id column (case-insensitive)
        user_id_cols = [col for col in df.columns if col.lower() == 'user_id']
        email_cols = [col for col in df.columns if col.lower() == 'email']
        
        print("\n► IDENTIFIER ANALYSIS")
        print("  ══════════════════")
        print(f"  • USER ID FIELDS: {', '.join(user_id_cols)}")
        print(f"  • EMAIL FIELDS: {', '.join(email_cols)}")
        
        # Identifier validation
        identifier_mask = pd.Series(False, index=df.index)
        
        if user_id_cols:
            identifier_mask |= df[user_id_cols[0]].notna()
        if email_cols:
            identifier_mask |= df[email_cols[0]].notna()
        
        missing_identifier_count = (~identifier_mask).sum()
        results['identifier'] = (identifier_mask, [
            f"MISSING IDENTIFIERS IN {missing_identifier_count} RECORDS"
        ])

        return results

    def _validate_user_identifier(self, df: pd.DataFrame) -> Tuple[pd.Series, list]:
        """Validate user identifier fields."""
        identifier_fields = ['user_id', 'username', 'email']
        available_fields = [field for field in identifier_fields if field in df.columns]
        
        if not available_fields:
            return pd.Series(False, index=df.index), ["No identifier fields (user_id/username/email) found"]
        
        # Check if at least one identifier field is present per record
        has_identifier = pd.Series(False, index=df.index)
        for field in available_fields:
            has_identifier |= df[field].notna()
        
        reasons = []
        if not has_identifier.all():
            count = (~has_identifier).sum()
            reasons.append(f"Missing identifier (user_id/username/email) for {count} records")
        
        return has_identifier, reasons

    def _validate_user_name(self, df: pd.DataFrame) -> Tuple[pd.Series, list]:
        """Validate user name fields."""
        # Check if we have full_name
        has_full_name = df['full_name'].notna()
        
        # Check if we have both first_name and last_name
        has_first_last = df['first_name'].notna() & df['last_name'].notna()
        
        # A record is valid if it has either full_name OR (first_name AND last_name)
        valid_names = has_full_name | has_first_last
        
        reasons = []
        if not valid_names.all():
            count = (~valid_names).sum()
            reasons.append(f"Missing name fields (either full_name or first_name+last_name) for {count} records")
            
        # After transformation, all records should have all three fields populated
        if df['full_name'].notna().any():  # If we have any full_name values
            missing_split = ~(df['first_name'].notna() & df['last_name'].notna())
            if missing_split.any():
                count = missing_split.sum()
                reasons.append(f"Failed to split full_name into first_name and last_name for {count} records")
        
        return valid_names, reasons

    def _validate_is_active(self, df: pd.DataFrame) -> Tuple[pd.Series, list]:
        """Validate is_active field."""
        valid_mask = pd.Series(True, index=df.index)
        reasons = []
        
        if 'is_active' in df.columns:
            # Check for null values
            null_mask = df['is_active'].isna()
            if null_mask.any():
                count = null_mask.sum()
                reasons.append(f"Found {count} records with null is_active values")
                valid_mask &= ~null_mask
            
            # Check for valid values (only 'Yes' or 'No' allowed)
            non_null_mask = ~null_mask
            if non_null_mask.any():
                valid_values = df.loc[non_null_mask, 'is_active'].isin(['Yes', 'No'])
                if not valid_values.all():
                    invalid_values = df.loc[non_null_mask & ~valid_values, 'is_active'].unique()
                    reasons.append(f"Invalid is_active values found: {invalid_values.tolist()}")
                    valid_mask &= ~(non_null_mask & ~valid_values)
        else:
            reasons.append("Missing required field: is_active")
            valid_mask = pd.Series(False, index=df.index)
        
        return valid_mask, reasons

    def _validate_dates(self, df: pd.DataFrame) -> Tuple[pd.Series, list]:
        """Validate datetime fields."""
        valid_mask = pd.Series(True, index=df.index)
        reasons = []
        datetime_fields = ['created_at', 'updated_at', 'last_login_at']
        
        for field in datetime_fields:
            if field in df.columns:
                non_null_dates = df[field].notna()
                if non_null_dates.any():
                    try:
                        parsed_dates = pd.to_datetime(df.loc[non_null_dates, field], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
                        invalid_dates = parsed_dates.isna()
                        if invalid_dates.any():
                            mask = non_null_dates & invalid_dates
                            valid_mask.loc[non_null_dates] &= ~invalid_dates
                            sample_invalid = df.loc[mask, field].head()
                            reasons.append(f"Invalid {field} format for {invalid_dates.sum()} records. Sample values: {sample_invalid.tolist()}")
                    except Exception as e:
                        reasons.append(f"Error parsing {field}: {str(e)}")
        
        return valid_mask, reasons

    def _validate_entity_tab(self, df: pd.DataFrame, tab_name: str) -> Tuple[pd.Series, list]:
        """Validate entity tab data."""
        # Initialize mask with DataFrame index
        valid_mask = pd.Series(True, index=df.index)
        reasons = []
        
        # Perform validations and update mask
        # ... your validation logic here ...
        
        return valid_mask, reasons

    def _validate_relationship_tab(self, df: pd.DataFrame, tab_name: str) -> Tuple[pd.Series, list]:
        """Validate relationship tab data."""
        # Initialize mask with DataFrame index
        valid_mask = pd.Series(True, index=df.index)
        reasons = []
        
        # Perform validations and update mask
        # ... your validation logic here ...
        
        return valid_mask, reasons

    def _validate_user_groups(self, df: pd.DataFrame) -> Tuple[pd.Series, List[str]]:
        """Validate User Groups relationship data."""
        valid_mask = pd.Series(True, index=df.index)
        reasons = []

        # Check required fields
        required_fields = ['user_id', 'group_id']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            valid_mask = pd.Series(False, index=df.index)
            reasons.append(f"Missing required fields: {', '.join(missing_fields)}")
            return valid_mask, reasons

        # Check for null values
        for field in required_fields:
            null_mask = df[field].isna()
            if null_mask.any():
                valid_mask &= ~null_mask
                count = null_mask.sum()
                reasons.append(f"Found {count} records with null {field}")

        # Check for duplicate relationships
        duplicates = df.duplicated(subset=['user_id', 'group_id'], keep='first')
        if duplicates.any():
            valid_mask &= ~duplicates
            count = duplicates.sum()
            reasons.append(f"Found {count} duplicate user-group relationships")

        return valid_mask, reasons
