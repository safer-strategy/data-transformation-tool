
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
            return pd.DataFrame()
        
        # Create new DataFrame with mapped columns
        transformed = pd.DataFrame()
        
        # Map columns according to mappings
        for source, target in mappings.items():
            if source in df.columns:
                transformed[target] = df[source].copy()
        
        # Special handling for different tabs
        if tab_name == "Users" and not transformed.empty:
            # Handle user identification fields
            if 'user_id' not in transformed.columns or transformed['user_id'].isna().all():
                if 'username' in transformed.columns and transformed['username'].notna().any():
                    transformed['user_id'] = transformed['username'].copy()
                elif 'email' in transformed.columns and transformed['email'].notna().any():
                    transformed['user_id'] = transformed['email'].copy()
            
            # Handle name fields
            if 'full_name' in transformed.columns and transformed['full_name'].notna().any():
                # Split full_name into first_name and last_name
                name_parts = transformed['full_name'].str.split(n=1, expand=True)
                transformed['first_name'] = name_parts[0]
                transformed['last_name'] = name_parts[1].fillna('')
            elif 'first_name' in transformed.columns and 'last_name' in transformed.columns:
                # Create full_name from first_name and last_name
                transformed['full_name'] = transformed['first_name'].fillna('') + ' ' + transformed['last_name'].fillna('')
                transformed['full_name'] = transformed['full_name'].str.strip()
            
            # Transform datetime fields to ISO 8601
            datetime_fields = ['created_at', 'updated_at', 'last_login_at']
            for field in datetime_fields:
                if field in transformed.columns:
                    transformed[field] = transformed[field].apply(self._transform_datetime_to_iso)
            
            # Transform is_active field to Yes/No
            if 'is_active' in transformed.columns:
                transformed['is_active'] = transformed['is_active'].apply(self._transform_boolean_to_yes_no)
        
        elif tab_name == "Groups" and not transformed.empty:
            # Add incremental group_id
            transformed['group_id'] = range(1, len(transformed) + 1)
            
            # Store mapping for relationship resolution
            for idx, row in transformed.iterrows():
                if 'group_name' in transformed.columns and pd.notna(row['group_name']):
                    self.group_id_map[str(row['group_name'])] = row['group_id']
        
        return transformed

    def _transform_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Users tab data."""
        # Handle full_name if first_name and last_name are present
        if 'first_name' in df.columns and 'last_name' in df.columns:
            df['full_name'] = df['first_name'] + ' ' + df['last_name']

        # Convert datetime fields
        date_fields = ['created_at', 'updated_at', 'last_login_at']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Handle is_active field
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].map({'Yes': 'true', 'No': 'false'})

        return df

    def _transform_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Groups tab data."""
        if df.empty:
            return df
        
        # Create a new DataFrame to avoid modifying the original
        transformed_df = df.copy()
        
        # If group_name is not present, return empty DataFrame
        if 'group_name' not in transformed_df.columns:
            return pd.DataFrame()
        
        return transformed_df

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
