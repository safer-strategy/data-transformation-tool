import pandas as pd
import logging
from typing import Dict, Tuple, Any

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
            # Always print tab processing header
            print(f"\n{'='*50}")
            print(f"Processing {tab_name}")
            print(f"{'='*50}")
            print(f"Total records: {len(df)}")

            if df.empty:
                print(f"Tab {tab_name} is empty, skipping validation")
                continue

            try:
                if tab_name == "Users":
                    # Perform each validation separately and collect results
                    validation_results = {
                        'identifier': self._validate_user_identifier(df),
                        'name': self._validate_user_name(df),
                        'is_active': self._validate_is_active(df),
                        'dates': self._validate_dates(df)
                    }

                    # Combine all validation masks
                    valid_mask = pd.Series(True, index=df.index)
                    all_reasons = []
                    
                    for check_name, (mask, reasons) in validation_results.items():
                        valid_mask &= mask
                        if reasons:
                            print(f"\n{check_name.upper()} Validation:")
                            for reason in reasons:
                                print(f"- {reason}")
                            all_reasons.extend(reasons)

                    # Print detailed statistics for invalid records
                    invalid_records = df[~valid_mask]
                    if not invalid_records.empty:
                        print(f"\nINVALID RECORDS SUMMARY:")
                        print(f"Total invalid records: {len(invalid_records)}")
                        
                        print("\nColumn-wise null value count:")
                        for col in invalid_records.columns:
                            null_count = invalid_records[col].isna().sum()
                            print(f"- {col}: {null_count} null values")
                        
                        print("\nSample of invalid records (first 5):")
                        print(invalid_records.head().to_string())

                elif tab_name in ["Groups", "Roles", "Resources"]:
                    valid_mask, reasons = self._validate_entity_tab(df, tab_name)
                else:
                    valid_mask, reasons = self._validate_relationship_tab(df, tab_name)

                valid_records = df[valid_mask]
                invalid_records = df[~valid_mask]

                print(f"\nValidation Summary for {tab_name}:")
                print(f"- Total records: {len(df)}")
                print(f"- Valid records: {len(valid_records)}")
                print(f"- Invalid records: {len(invalid_records)}")

                valid_data[tab_name] = valid_records
                if not invalid_records.empty:
                    invalid_data[tab_name] = invalid_records

            except Exception as e:
                print(f"Error validating {tab_name}: {str(e)}")
                raise

        return valid_data, invalid_data

    def _validate_user_identifier(self, df: pd.DataFrame) -> Tuple[pd.Series, list]:
        """Validate user identifier fields."""
        has_identifier = (
            df['user_id'].notna() |
            df['username'].notna() |
            df['email'].notna()
        )
        reasons = []
        if not has_identifier.all():
            count = (~has_identifier).sum()
            reasons.append(f"Missing identifier (user_id/username/email) for {count} records")
        return has_identifier, reasons

    def _validate_user_name(self, df: pd.DataFrame) -> Tuple[pd.Series, list]:
        """Validate user name fields."""
        has_full_name = df['full_name'].notna()
        has_first_last = df['first_name'].notna() & df['last_name'].notna()
        valid_names = has_full_name | has_first_last
        reasons = []
        if not valid_names.all():
            count = (~valid_names).sum()
            reasons.append(f"Missing name fields (either full_name or first_name+last_name) for {count} records")
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
        """Validate entity tabs (Groups, Roles, Resources)."""
        valid_mask = pd.Series(True, index=df.index)
        reasons = []

        # Add specific validation logic for entity tabs
        schema_fields = self.schema[tab_name].keys()
        for field in schema_fields:
            if self.schema[tab_name][field].get('mandatory', False):
                field_valid = df[field].notna()
                if not field_valid.all():
                    mask = ~field_valid
                    valid_mask &= field_valid
                    reasons.append(f"Missing mandatory field {field} for {mask.sum()} records")

        return valid_mask, reasons

    def _validate_relationship_tab(self, df: pd.DataFrame, tab_name: str) -> Tuple[pd.Series, list]:
        """Validate relationship tabs."""
        valid_mask = pd.Series(True, index=df.index)
        reasons = []

        # Check for missing values in all columns
        for col in df.columns:
            col_valid = df[col].notna()
            if not col_valid.all():
                mask = ~col_valid
                valid_mask &= col_valid
                reasons.append(f"Missing values in {col} for {mask.sum()} records")

        return valid_mask, reasons