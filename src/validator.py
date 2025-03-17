import sys
import pandas as pd
from typing import Dict, Tuple, List
import logging
from colorama import Fore, Style, init

init(autoreset=True)

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
            print(f"\n╔{'═'*50}╗")
            print(f"║ VALIDATING SIGNAL: {tab_name:<39}║")
            print(f"║ RECORDS DETECTED: {len(df):<39}║")
            print(f"╚{'═'*50}╝")

            if df.empty:
                print(f"\n► WARNING: SIGNAL {tab_name} IS EMPTY - BYPASSING VALIDATION")
                continue

            try:
                valid_mask = pd.Series(True, index=df.index)

                if tab_name == "Users" or tab_name == "Flattened":
                    validation_results = self._validate_users(df)
                elif tab_name == "User Groups":
                    validation_results = self._validate_user_groups(df)
                elif tab_name in ["Groups", "Roles", "Resources"]:
                    validation_results = self._validate_entity_tab(df, tab_name)
                else:
                    validation_results = self._validate_relationship_tab(df, tab_name)

                print("\n► VALIDATION RESULTS")
                print("  ═════════════════")
                for check, (check_mask, reasons) in validation_results.items():
                    print(f"\n  ▶ {check.upper()} CHECK:")
                    for reason in reasons:
                        print(f"    • {reason}")
                    valid_mask &= check_mask

                valid_records = df[valid_mask]
                invalid_records = df[~valid_mask]

                print(f"\n► VALIDATION SUMMARY FOR {tab_name}")
                print("  ═══════════════════════════")
                print(f"  • TOTAL RECORDS:    {len(df)}")
                print(f"  • VALID RECORDS:    {len(valid_records)}")
                print(f"  • INVALID RECORDS:  {len(invalid_records)}")

                if len(valid_records) > 0:
                    valid_data[tab_name] = valid_records
                if len(invalid_records) > 0:
                    invalid_data[tab_name] = invalid_records

            except Exception as e:
                print(f"\n► ERROR: VALIDATION FAILED FOR {tab_name}")
                print(f"  • REASON: {str(e)}")
                raise

        return valid_data, invalid_data

    def _validate_users(self, df: pd.DataFrame) -> Dict[str, Tuple[pd.Series, List[str]]]:
        """Validate Users tab data."""
        validation_results = {}
        
        print(f"\n{Fore.CYAN}╔══════════════════ VALIDATION SEQUENCE ══════════════════╗")
        print(f"║ SIGNAL TYPE: Users                                        ║")
        print(f"║ RECORDS: {len(df):<52}║")
        print(f"╚═══════════════════════════════════════════════════════════╝{Style.RESET_ALL}")

        # Identifier Check
        identifier_reasons = []
        user_id_mask = df['user_id'].notna()
        email_mask = df['email'].notna()
        identifier_mask = user_id_mask | email_mask
        
        if (~identifier_mask).any():
            identifier_reasons.append(f"Missing identifiers in {(~identifier_mask).sum()} records")
        validation_results['identifier'] = (identifier_mask, identifier_reasons)

        # Name Check
        name_reasons = []
        name_mask = df['first_name'].notna() & df['last_name'].notna()
        if (~name_mask).any():
            name_reasons.append(f"Missing name components in {(~name_mask).sum()} records")
        validation_results['name'] = (name_mask, name_reasons)

        # Status Check
        status_reasons = []
        valid_statuses = {'Yes', 'No'}  # Updated to accept Yes/No values
        status_mask = df['is_active'].isin(valid_statuses)
        if (~status_mask).any():
            invalid_statuses = df[~status_mask]['is_active'].unique()
            status_reasons.append(f"Invalid status values found: {', '.join(map(str, invalid_statuses))}")
        validation_results['status'] = (status_mask, status_reasons)

        return validation_results

    def _validate_entity_tab(self, df: pd.DataFrame, tab_name: str) -> Dict[str, Tuple[pd.Series, List[str]]]:
        """Validate entity tabs (Roles, Groups, Resources)."""
        validation_results = {}
        
        # Check for required identifier fields based on tab type
        if tab_name == "Roles":
            id_fields = ['role_id', 'role_name']
        elif tab_name == "Groups":
            id_fields = ['group_id', 'group_name']
        elif tab_name == "Resources":
            id_fields = ['resource_id', 'resource_name']
        else:
            return {'error': (pd.Series(False, index=df.index), [f"Unknown entity tab: {tab_name}"])}

        # Validate identifier fields
        identifier_reasons = []
        identifier_mask = pd.Series(False, index=df.index)
        
        available_fields = [field for field in id_fields if field in df.columns]
        if not available_fields:
            identifier_reasons.append(f"No identifier fields found. Required: {' or '.join(id_fields)}")
        else:
            for field in available_fields:
                identifier_mask |= df[field].notna()
            
            if (~identifier_mask).any():
                missing_count = (~identifier_mask).sum()
                identifier_reasons.append(f"Missing identifiers in {missing_count} records")
        
        validation_results['identifier'] = (identifier_mask, identifier_reasons)

        # Validate name fields if present
        name_field = f"{tab_name.lower()[:-1]}_name"  # e.g., role_name, group_name
        if name_field in df.columns:
            name_mask = df[name_field].notna()
            name_reasons = []
            if (~name_mask).any():
                missing_count = (~name_mask).sum()
                name_reasons.append(f"Missing {name_field} in {missing_count} records")
            validation_results['name'] = (name_mask, name_reasons)

        return validation_results

    def _validate_relationship_tab(self, df: pd.DataFrame, tab_name: str) -> Dict[str, Tuple[pd.Series, List[str]]]:
        """Validate relationship tabs (User Roles, User Groups, Role Resources)."""
        validation_results = {}
        
        # Define required fields based on tab type
        if tab_name == "User Roles":
            required_fields = ['user_id', 'role_id']
        elif tab_name == "User Groups":
            required_fields = ['user_id', 'group_id']
        elif tab_name == "Role Resources":
            required_fields = ['role_id', 'resource_id']
        else:
            return {'error': (pd.Series(False, index=df.index), [f"Unknown relationship tab: {tab_name}"])}

        # Check for required fields
        field_reasons = []
        field_mask = pd.Series(True, index=df.index)
        
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            field_reasons.append(f"Missing required fields: {', '.join(missing_fields)}")
            field_mask = pd.Series(False, index=df.index)
        
        # Check for null values in required fields
        if not missing_fields:
            for field in required_fields:
                null_mask = df[field].isna()
                if null_mask.any():
                    field_mask &= ~null_mask
                    count = null_mask.sum()
                    field_reasons.append(f"Found {count} records with null {field}")
        
        validation_results['fields'] = (field_mask, field_reasons)

        # Check for duplicate relationships
        duplicate_reasons = []
        duplicate_mask = pd.Series(True, index=df.index)
        
        duplicates = df.duplicated(subset=required_fields, keep='first')
        if duplicates.any():
            duplicate_mask &= ~duplicates
            count = duplicates.sum()
            duplicate_reasons.append(f"Found {count} duplicate relationships")
        
        validation_results['duplicates'] = (duplicate_mask, duplicate_reasons)

        return validation_results

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
