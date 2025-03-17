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
        """Validate all data according to schema rules."""
        valid_data = {}
        invalid_data = {}

        for tab_name, df in data.items():
            self.stdout.write(f"\n{'='*50}\nProcessing {tab_name}\n{'='*50}\n")
            self.stdout.write(f"Total records: {len(df)}\n")
            self.stdout.flush()

            if df.empty:
                self.stdout.write(f"Tab {tab_name} is empty, skipping validation\n")
                self.stdout.flush()
                continue

            try:
                # Initialize valid_mask before any validation
                valid_mask = pd.Series(True, index=df.index)

                if tab_name == "Users" or tab_name == "Flattened":
                    validation_results = self._validate_users(df)
                elif tab_name == "User Groups":
                    validation_results = self._validate_user_groups(df)
                elif tab_name in ["Groups", "Roles", "Resources"]:
                    validation_results = self._validate_entity_tab(df, tab_name)
                else:
                    validation_results = self._validate_relationship_tab(df, tab_name)

                # Print validation results immediately
                self.stdout.write("\nValidation Results:\n")
                for check, (check_mask, reasons) in validation_results.items():
                    self.stdout.write(f"\n{check} check:\n")
                    for reason in reasons:
                        self.stdout.write(f"- {reason}\n")
                    valid_mask &= check_mask
                self.stdout.flush()

                # Split data based on validation results
                valid_records = df[valid_mask]
                invalid_records = df[~valid_mask]

                # Log results
                self.stdout.write(f"\nValidation Summary for {tab_name}:\n")
                self.stdout.write(f"- Total records: {len(df)}\n")
                self.stdout.write(f"- Valid records: {len(valid_records)}\n")
                self.stdout.write(f"- Invalid records: {len(invalid_records)}\n")
                self.stdout.flush()

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
        
        # Debug print the actual columns
        print("\nActual columns in DataFrame:", df.columns.tolist())
        
        # Check for user_id column (case-insensitive)
        user_id_cols = [col for col in df.columns if col.lower() == 'user_id']
        email_cols = [col for col in df.columns if col.lower() == 'email']
        
        # Debug print
        print("Found user_id columns:", user_id_cols)
        print("Found email columns:", email_cols)
        
        # Identifier validation
        identifier_mask = pd.Series(False, index=df.index)
        
        if user_id_cols:
            identifier_mask |= df[user_id_cols[0]].notna()
        if email_cols:
            identifier_mask |= df[email_cols[0]].notna()
        
        missing_identifier_count = (~identifier_mask).sum()
        results['identifier'] = (identifier_mask, [
            f"Missing identifiers (User ID or Email) in {missing_identifier_count} records"
        ])

        # Name validation (simplified)
        results['name'] = (pd.Series(True, index=df.index), [])
        
        # Status validation (simplified)
        results['status'] = (pd.Series(True, index=df.index), [])

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

    def _validate_user_groups(self, df: pd.DataFrame) -> Dict:
        """Validate User Groups relationship data."""
        results = {}
        
        # Check for required columns
        required_cols = ['user_id', 'group_id']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            results['columns'] = (pd.Series(False, index=df.index), 
                                [f"Missing required columns: {', '.join(missing_cols)}"])
            return results

        # Validate user_id
        user_id_mask = df['user_id'].notna()
        user_id_missing = (~user_id_mask).sum()
        results['user_id'] = (user_id_mask, 
                             [f"Missing user_id in {user_id_missing} records"])

        # Validate group_id
        group_id_mask = df['group_id'].notna()
        group_id_missing = (~group_id_mask).sum()
        results['group_id'] = (group_id_mask, 
                              [f"Missing group_id in {group_id_missing} records"])

        return results

    def _is_valid_iso_datetime(self, dt_str: str) -> bool:
        """Check if string is valid ISO 8601 datetime."""
        if not isinstance(dt_str, str):
            return False
        try:
            datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
            return True
        except ValueError:
            return False
