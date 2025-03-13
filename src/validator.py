import sys
import pandas as pd
from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class Validator:
    """Validates transformed data against schema rules."""

    def __init__(self, schema: Dict):
        """Initialize validator with schema.

        Args:
            schema: The schema dictionary
        """
        self.schema = schema
        # Force stdout for immediate output
        self.stdout = sys.stdout

    def validate_data(self, data: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        """Validate transformed data against schema rules."""
        valid_data = {}
        invalid_data = {}

        for tab_name, df in data.items():
            # Force print to stdout
            self.stdout.write(f"\n{'='*50}\n")
            self.stdout.write(f"Processing {tab_name}\n")
            self.stdout.write(f"{'='*50}\n")
            self.stdout.write(f"Total records: {len(df)}\n")
            self.stdout.flush()  # Force flush the output

            if df.empty:
                self.stdout.write(f"Tab {tab_name} is empty, skipping validation\n")
                self.stdout.flush()
                continue

            try:
                if tab_name == "Users":
                    # Perform validations
                    validation_results = self._validate_users(df)
                    
                    # Print validation results immediately
                    self.stdout.write("\nValidation Results for Users:\n")
                    for check, (is_valid, reasons) in validation_results.items():
                        self.stdout.write(f"\n{check} check:\n")
                        for reason in reasons:
                            self.stdout.write(f"- {reason}\n")
                    self.stdout.flush()

                    # Calculate final valid mask
                    valid_mask = pd.Series(True, index=df.index)
                    for _, (is_valid, _) in validation_results.items():
                        valid_mask &= is_valid

                    # Print invalid records summary
                    invalid_records = df[~valid_mask]
                    if not invalid_records.empty:
                        self.stdout.write(f"\nInvalid Records Summary:\n")
                        self.stdout.write(f"Total invalid records: {len(invalid_records)}\n")
                        self.stdout.write("\nColumn-wise null counts:\n")
                        for col in invalid_records.columns:
                            null_count = invalid_records[col].isna().sum()
                            self.stdout.write(f"{col}: {null_count} null values\n")
                        self.stdout.write("\nFirst 5 invalid records:\n")
                        self.stdout.write(invalid_records.head().to_string())
                        self.stdout.write("\n")
                    self.stdout.flush()

                # Store results
                valid_records = df[valid_mask]
                invalid_records = df[~valid_mask]
                
                if len(valid_records) > 0:
                    valid_data[tab_name] = valid_records
                if len(invalid_records) > 0:
                    invalid_data[tab_name] = invalid_records

            except Exception as e:
                self.stdout.write(f"Error validating {tab_name}: {str(e)}\n")
                self.stdout.flush()
                raise

        return valid_data, invalid_data

    def _validate_users(self, df: pd.DataFrame) -> Dict:
        """Validate users tab with detailed output."""
        results = {}
        
        # Identifier validation
        has_identifier = (
            df['user_id'].notna() |
            df['username'].notna() |
            df['email'].notna()
        )
        missing_identifier_count = (~has_identifier).sum()
        results['identifier'] = (has_identifier, [
            f"Missing identifier (user_id/username/email) for {missing_identifier_count} records"
        ] if missing_identifier_count > 0 else [])

        # Name validation
        has_full_name = df['full_name'].notna()
        has_first_last = df['first_name'].notna() & df['last_name'].notna()
        valid_names = has_full_name | has_first_last
        missing_names_count = (~valid_names).sum()
        results['names'] = (valid_names, [
            f"Missing name fields (full_name or first_name+last_name) for {missing_names_count} records"
        ] if missing_names_count > 0 else [])

        # Active status validation
        valid_active = pd.Series(True, index=df.index)
        active_reasons = []
        if 'is_active' in df.columns:
            non_null_active = df['is_active'].notna()
            if non_null_active.any():
                valid_values = df.loc[non_null_active, 'is_active'].isin(['Yes', 'No'])
                if not valid_values.all():
                    invalid_values = df.loc[non_null_active & ~valid_values, 'is_active'].unique()
                    active_reasons.append(f"Invalid is_active values found: {invalid_values.tolist()}")
                    valid_active.loc[non_null_active] &= valid_values
        results['active_status'] = (valid_active, active_reasons)

        return results

    def _validate_groups(self, df: pd.DataFrame) -> pd.Series:
        """Validate Groups tab according to schema rules."""
        invalid_mask = pd.Series(False, index=df.index)
        
        # Check that at least group_id or group_name is present
        id_columns = ['group_id', 'group_name']
        available_ids = [col for col in id_columns if col in df.columns]
        if available_ids:
            identifiers_mask = df[available_ids].isna().all(axis=1)
            invalid_mask |= identifiers_mask
        
        return invalid_mask

    def _validate_roles(self, df: pd.DataFrame) -> pd.Series:
        """Validate Roles tab according to schema rules."""
        invalid_mask = pd.Series(False, index=df.index)
        
        # Check that at least role_id or role_name is present
        id_columns = ['role_id', 'role_name']
        available_ids = [col for col in id_columns if col in df.columns]
        if available_ids:
            identifiers_mask = df[available_ids].isna().all(axis=1)
            invalid_mask |= identifiers_mask
        
        return invalid_mask

    def _validate_resources(self, df: pd.DataFrame) -> pd.Series:
        """Validate Resources tab according to schema rules."""
        invalid_mask = pd.Series(False, index=df.index)
        
        # Check that at least resource_id or resource_name is present
        id_columns = ['resource_id', 'resource_name']
        available_ids = [col for col in id_columns if col in df.columns]
        if available_ids:
            identifiers_mask = df[available_ids].isna().all(axis=1)
            invalid_mask |= identifiers_mask
        
        return invalid_mask

    def _validate_relationship_tab(self, df: pd.DataFrame, tab_name: str) -> pd.Series:
        """Validate relationship tabs according to schema rules."""
        invalid_mask = pd.Series(False, index=df.index)
        
        # Only validate fields that are present in the DataFrame
        if tab_name in self.schema:
            schema_fields = self.schema[tab_name].keys()
            available_fields = [field for field in schema_fields if field in df.columns]
            for field in available_fields:
                invalid_mask |= df[field].isna()
        
        return invalid_mask

    def _is_valid_iso_datetime(self, dt_str: str) -> bool:
        """Check if string is valid ISO 8601 datetime."""
        if not isinstance(dt_str, str):
            return False
        try:
            datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
            return True
        except ValueError:
            return False
