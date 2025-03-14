
import pandas as pd
import logging
from typing import Dict, Any, List
from datetime import datetime
import json
import hashlib

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, schema_file: str):
        self.logger = logging.getLogger(__name__)
        with open(schema_file) as f:
            self.schema = json.load(f)
        self.group_id_map = {}  # Store mapping between original group identifiers and new incremental IDs

    def transform_data_tab(self, df: pd.DataFrame, mappings: Dict[str, str], tab_name: str) -> pd.DataFrame:
        """Transform data for a specific tab."""
        if df.empty:
            if tab_name == "Groups":
                return pd.DataFrame(columns=['group_id', 'group_name', 'group_description'])
            return pd.DataFrame()

        # Create new DataFrame with mapped columns
        transformed = pd.DataFrame()
        
        # Map columns according to mappings
        for source, target in mappings.items():
            if source in df.columns:
                transformed[target] = df[source].copy()
        
        # Special handling for different tabs
        if tab_name == "Groups":
            return self._transform_groups(transformed)
        elif tab_name == "Users":
            return self._transform_users(transformed)
        elif tab_name == "Roles":
            return self._transform_roles(transformed)
        elif tab_name == "Resources":
            return self._transform_resources(transformed)
        else:
            return self._transform_relationships(transformed, tab_name)

    def _transform_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Users tab data."""
        # Ensure all required columns exist
        required_cols = ['user_id', 'username', 'email', 'first_name', 'last_name', 'full_name']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        # Handle identifiers according to schema rules
        if df['user_id'].isna().any():
            # If user_id is missing, try to populate from email or username
            mask = df['user_id'].isna()
            df.loc[mask, 'user_id'] = df.loc[mask].apply(
                lambda row: row['email'] if pd.notna(row['email']) 
                else (row['username'] if pd.notna(row['username']) else None),
                axis=1
            )

        if df['username'].isna().any():
            # If username is missing, populate from email
            mask = df['username'].isna()
            df.loc[mask, 'username'] = df.loc[mask, 'email']

        # Handle name fields
        if 'first_name' in df.columns and 'last_name' in df.columns:
            # Create full_name from first_name and last_name if missing
            mask = df['full_name'].isna() & df['first_name'].notna() & df['last_name'].notna()
            df.loc[mask, 'full_name'] = df.loc[mask, 'first_name'] + ' ' + df.loc[mask, 'last_name']

        # Convert datetime fields
        date_fields = ['created_at', 'updated_at', 'last_login_at']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Handle is_active field
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].map({'Yes': 'Yes', 'No': 'No'})

        return df

    def _transform_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Groups tab data according to schema rules."""
        # Define the exact column order
        COLUMN_ORDER = ['group_id', 'group_name', 'group_description']
        
        if df.empty:
            return pd.DataFrame(columns=COLUMN_ORDER)

        # Create a new empty DataFrame with the correct column order
        result = pd.DataFrame(columns=COLUMN_ORDER)
        
        # Populate data in the correct order
        result.loc[:, 'group_id'] = range(1, len(df) + 1)
        result.loc[:, 'group_name'] = df['group_name'] if 'group_name' in df.columns else ''
        result.loc[:, 'group_description'] = (
            df['group_description'] if 'group_description' in df.columns 
            else df['group_name'] if 'group_name' in df.columns 
            else ''
        )
        
        # Fill missing descriptions with group names
        mask = result['group_description'].isna()
        result.loc[mask, 'group_description'] = result.loc[mask, 'group_name']
        
        # Store mapping for relationship resolution
        for idx, row in result.iterrows():
            if pd.notna(row['group_name']):
                self.group_id_map[str(row['group_name'])] = row['group_id']
        
        # Final force of column order
        return result[COLUMN_ORDER].copy()

    def _transform_roles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Roles tab data."""
        # If role_id is missing but role_name is present, use role_name as role_id
        if 'role_id' not in df.columns and 'role_name' in df.columns:
            df['role_id'] = df['role_name']
        return df

    def _transform_resources(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Resources tab data."""
        # If resource_id is missing but resource_name is present, use resource_name as resource_id
        if 'resource_id' not in df.columns and 'resource_name' in df.columns:
            df['resource_id'] = df['resource_name']
        return df

    def _transform_relationships(self, df: pd.DataFrame, tab_name: str) -> pd.DataFrame:
        """Transform relationship tab data."""
        if df.empty:
            return df
            
        transformed_df = df.copy()
        
        if tab_name == "User Groups" and 'group_id' in transformed_df.columns:
            # Map the group_id to the new incremental IDs
            transformed_df['group_id'] = transformed_df['group_id'].apply(
                lambda x: self.group_id_map.get(str(x), x) if pd.notna(x) else x
            )
            
        return transformed_df

    def resolve_relationships(self, entity_data: Dict[str, pd.DataFrame], rel_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Resolve relationships using transformed entity data."""
        # First transform the Groups data to establish group_id mapping
        if 'Groups' in entity_data:
            entity_data['Groups'] = self._transform_groups(entity_data['Groups'])
        
        transformed_rel = {}
        for rel_name, rel_df in rel_data.items():
            transformed_df = self._transform_relationships(rel_df, rel_name)
            transformed_rel[rel_name] = transformed_df
            
        return transformed_rel

    def _transform_boolean_to_yes_no(self, value) -> str:
        """Transform various status values to Yes/No format."""
        if pd.isna(value):
            return "No"
        
        value_str = str(value).lower().strip()
        
        # Map various active status values
        active_values = {
            'true', '1', 'yes', 'y', 't', 'active', 'enabled'
        }
        inactive_values = {
            'false', '0', 'no', 'n', 'f', 'inactive', 'disabled',
            'deactivated', 'partially deactivated'
        }
        
        if value_str in active_values:
            return "Yes"
        elif value_str in inactive_values:
            return "No"
        
        # Log unexpected values
        self.logger.warning(f"Unexpected is_active value: '{value}', defaulting to 'No'")
        return "No"

    def _transform_datetime_to_iso(self, value) -> str:
        """Transform datetime to ISO 8601 format."""
        if pd.isna(value):
            return None
        
        try:
            if isinstance(value, str):
                # Try common datetime formats
                for fmt in [
                    "%Y-%m-%d %H:%M:%S",
                    "%d/%m/%Y %H:%M:%S",
                    "%m/%d/%Y %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d"
                ]:
                    try:
                        dt = datetime.strptime(value, fmt)
                        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except ValueError:
                        continue
            elif isinstance(value, (datetime, pd.Timestamp)):
                return value.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            self.logger.warning(f"Failed to parse datetime value '{value}': {str(e)}")
        
        return None
