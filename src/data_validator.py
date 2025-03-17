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
            try:
                self.logger.info(f"Validating {tab_name} with {len(df)} records")
                
                if tab_name == "Users":
                    valid_mask, reasons = self._validate_users(df)
                elif tab_name == "Groups":
                    valid_mask, reasons = self._validate_groups(df)
                elif tab_name == "User Groups":
                    valid_mask, reasons = self._validate_user_groups(df)
                else:
                    valid_mask, reasons = self._validate_relationship_tab(df, tab_name)

                valid_records = df[valid_mask]
                invalid_records = df[~valid_mask]

                if not valid_records.empty:
                    valid_data[tab_name] = valid_records
                if not invalid_records.empty:
                    invalid_data[tab_name] = invalid_records

                # Log validation results
                self.logger.info(f"Validation results for {tab_name}:")
                self.logger.info(f"- Valid records: {len(valid_records)}")
                self.logger.info(f"- Invalid records: {len(invalid_records)}")
                for reason in reasons:
                    self.logger.info(f"- {reason}")

            except Exception as e:
                self.logger.error(f"Error validating {tab_name}: {str(e)}")
                raise

        return valid_data, invalid_data

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
            non_null_active = df['is_active'].notna()
            if non_null_active.any():
                valid_active = df.loc[non_null_active, 'is_active'].isin(['Yes', 'No'])
                if not valid_active.all():
                    invalid_values = df.loc[non_null_active & ~valid_active, 'is_active'].unique()
                    reasons.append(f"Invalid is_active values found: {invalid_values.tolist()}")
                    valid_mask.loc[non_null_active] &= valid_active
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
