import pandas as pd
import logging
from typing import Dict, Any, List
from datetime import datetime
import json
import hashlib

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, schema_path: str):
        with open(schema_path) as f:
            self.schema = json.load(f)
        self.group_id_map = {}
        self.logger = logging.getLogger(__name__)

    def transform_data(self, data: Dict[str, pd.DataFrame], mappings: Dict[str, Dict]) -> Dict[str, pd.DataFrame]:
        """Transform all data according to schema rules."""
        result = {}
        
        # Process tabs in specific order
        processing_order = ['Users', 'Groups', 'Roles', 'Resources', 'User Groups', 'User Roles', 
                           'Group Roles', 'User Resources', 'Role Resources', 'Group Resources']
        
        for tab_name in processing_order:
            if tab_name not in data:
                continue
            
            df = data[tab_name]
            tab_mapping = mappings.get(tab_name, {})
            
            # Add these debug lines
            self.logger.debug(f"Processing {tab_name}")
            self.logger.debug(f"Input DataFrame shape: {df.shape}")
            self.logger.debug(f"Input columns: {df.columns.tolist()}")
            
            try:
                if tab_name == 'Users':
                    result[tab_name] = self._transform_users_tab(df, tab_mapping)
                elif tab_name == 'Groups':
                    result[tab_name] = self._transform_groups_tab(df, tab_mapping)
                elif tab_name == 'User Groups':
                    result[tab_name] = self._transform_user_groups_tab(df, tab_mapping)
                else:
                    result[tab_name] = self._transform_tab(df, tab_name, tab_mapping)
                
                # Add these debug lines
                self.logger.debug(f"Output DataFrame shape: {result[tab_name].shape}")
                self.logger.debug(f"Output columns: {result[tab_name].columns.tolist()}")
                self.logger.info(f"Successfully transformed {tab_name} tab")
            
            except Exception as e:
                self.logger.error(f"Error transforming {tab_name} tab: {str(e)}")
                raise
        
        return result

    def _transform_user_groups_tab(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform User Groups tab with automatic ID resolution."""
        self.logger.info("Starting User Groups transformation")
        self.logger.info(f"Initial columns: {df.columns.tolist()}")
        
        # First apply the column mapping
        result_df = df.rename(columns=mapping)
        self.logger.info(f"Columns after mapping: {result_df.columns.tolist()}")
        
        # Handle group_id conversion
        if 'group_id' in result_df.columns:
            original_group_ids = result_df['group_id'].copy()
            # Convert group names to IDs using the mapping
            result_df['group_id'] = result_df['group_id'].apply(
                lambda x: self.group_id_map.get(str(x).strip(), x) if pd.notna(x) else None
            )
            # Log the conversions
            for orig, new in zip(original_group_ids, result_df['group_id']):
                self.logger.info(f"Group conversion: {orig} -> {new}")
        
        # Ensure required columns exist
        required_columns = ['user_id', 'group_id']
        for col in required_columns:
            if col not in result_df.columns:
                result_df[col] = None
        
        # Reorder columns
        result_df = result_df[required_columns]
        
        return result_df

    def _transform_groups_tab(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform Groups tab and establish group ID mappings."""
        try:
            self.logger.info("Starting Groups transformation")
            self.logger.info(f"Initial columns: {df.columns.tolist()}")
            self.logger.info(f"Mapping being applied: {mapping}")
            
            # Apply column mapping
            result_df = df.rename(columns=mapping)
            self.logger.info(f"Columns after mapping: {result_df.columns.tolist()}")
            
            # Handle the special case of __generated_group_id__
            if '__generated_group_id__' in mapping:
                if 'group_name' not in result_df.columns:
                    raise ValueError("Cannot generate group_id: group_name column not found")
                    
                # Generate group IDs for rows where group_name exists
                result_df['group_id'] = result_df['group_name'].apply(self._get_or_create_group_id)
                
                # Log the generated IDs
                self.logger.info("Generated group IDs:")
                for name, gid in zip(result_df['group_name'], result_df['group_id']):
                    self.logger.info(f"  {name} -> {gid}")
            
            # Ensure required columns exist
            required_columns = ['group_id', 'group_name']
            for col in required_columns:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # Build group_id_map from valid rows
            valid_mask = result_df['group_name'].notna() & result_df['group_id'].notna()
            valid_rows = result_df[valid_mask]
            
            # Update the group_id_map
            new_mappings = dict(zip(
                valid_rows['group_name'].astype(str).str.strip(),
                valid_rows['group_id']
            ))
            self.group_id_map.update(new_mappings)
            
            self.logger.info(f"Updated group_id_map: {self.group_id_map}")
            
            # Return only the required columns in the correct order
            return result_df[required_columns]
            
        except Exception as e:
            self.logger.error(f"Error in _transform_groups_tab: {str(e)}")
            self.logger.error(f"Input columns: {df.columns.tolist()}")
            self.logger.error(f"Mapping: {mapping}")
            raise

    def _transform_tab(self, df: pd.DataFrame, tab_name: str, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform a generic tab according to schema rules."""
        self.logger.info(f"Starting transformation for tab: {tab_name}")
        self.logger.info(f"Initial columns: {df.columns.tolist()}")
        
        # Verify mapping is a dictionary
        if not isinstance(mapping, dict):
            self.logger.error(f"Invalid mapping type: {type(mapping)}. Expected dict.")
            raise TypeError(f"Mapping must be a dictionary, got {type(mapping)}")
        
        # Apply column mapping
        result_df = df.rename(columns=mapping)
        self.logger.info(f"Columns after mapping: {result_df.columns.tolist()}")
        
        return result_df

    def _transform_users_tab(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform Users tab with strict schema rule compliance."""
        self.logger.info(f"Starting Users transformation with columns: {df.columns.tolist()}")
        self.logger.info(f"Mapping being applied: {mapping}")
        
        try:
            # Create a copy of the input DataFrame
            final_df = df.copy()
            
            # Apply column mapping while preserving original data
            final_df = final_df.rename(columns=mapping)
            
            # Initialize required columns if they don't exist
            required_columns = [
                'user_id', 'username', 'email', 'first_name', 'last_name', 
                'full_name', 'is_active', 'created_at', 'updated_at', 'last_login_at'
            ]
            
            for col in required_columns:
                if col not in final_df.columns:
                    final_df[col] = None

            # Ensure username and email are properly copied from input
            if 'username' in mapping.values():
                orig_username_col = [k for k, v in mapping.items() if v == 'username'][0]
                final_df['username'] = df[orig_username_col]
            
            if 'email' in mapping.values():
                orig_email_col = [k for k, v in mapping.items() if v == 'email'][0]
                final_df['email'] = df[orig_email_col]

            # Handle user_id according to schema rules
            missing_user_id_mask = final_df['user_id'].isna()
            if missing_user_id_mask.any():
                # If username exists but user_id is missing, copy username to user_id
                username_mask = missing_user_id_mask & final_df['username'].notna()
                final_df.loc[username_mask, 'user_id'] = final_df.loc[username_mask, 'username']
                
                # For remaining missing user_ids, try email
                still_missing_mask = final_df['user_id'].isna()
                email_mask = still_missing_mask & final_df['email'].notna()
                final_df.loc[email_mask, 'user_id'] = final_df.loc[email_mask, 'email']

            # Convert boolean is_active to Yes/No
            if 'is_active' in final_df.columns:
                final_df['is_active'] = final_df['is_active'].map({True: 'Yes', False: 'No'})
                final_df['is_active'] = final_df['is_active'].fillna('No')

            # Format datetime fields
            datetime_fields = ['created_at', 'updated_at', 'last_login_at']
            for field in datetime_fields:
                if field in final_df.columns and not final_df[field].isna().all():
                    final_df[field] = pd.to_datetime(final_df[field]).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Extract name from email if no name fields exist
            missing_names_mask = (
                final_df['full_name'].isna() & 
                (final_df['first_name'].isna() | final_df['last_name'].isna())
            )
            
            if missing_names_mask.any():
                email_names = final_df.loc[missing_names_mask, 'email'].apply(
                    lambda x: ' '.join(x.split('@')[0].split('_')) if pd.notna(x) else None
                )
                final_df.loc[missing_names_mask, 'full_name'] = email_names

            # Log the state of username field
            self.logger.info(f"Username null count before return: {final_df['username'].isna().sum()}")
            self.logger.info(f"First few usernames: {final_df['username'].head()}")

            return final_df[required_columns]

        except Exception as e:
            self.logger.error(f"Error in Users transformation: {str(e)}")
            self.logger.error(f"Current columns: {final_df.columns.tolist() if 'final_df' in locals() else 'N/A'}")
            raise

    def _transform_roles_tab(self, df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
        """Transform Roles tab ensuring all required columns exist."""
        # If permissions is a column and contains non-null values, ensure it's a string
        if 'permissions' in df.columns:
            df['permissions'] = df['permissions'].apply(
                lambda x: str(x) if pd.notna(x) else None
            )
        return df

    def _transform_resources_tab(self, df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
        """Transform Resources tab ensuring all required columns exist."""
        return df

    def _get_or_create_group_id(self, group_name: str) -> str:
        """Generate or retrieve a group ID for a given group name."""
        if pd.isna(group_name):
            return None
            
        group_name = str(group_name).strip()
        if group_name in self.group_id_map:
            return self.group_id_map[group_name]
            
        # Generate a new ID using a hash function
        hash_object = hashlib.md5(group_name.encode())
        new_id = hash_object.hexdigest()[:8]  # Use first 8 characters of MD5 hash
        
        self.group_id_map[group_name] = new_id
        return new_id

    def resolve_relationships(self, entity_data: Dict[str, pd.DataFrame], relationship_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Resolve relationships between entities using lookups.
        
        Args:
            entity_data: Dictionary of entity DataFrames (Users, Groups, etc.)
            relationship_data: Dictionary of relationship DataFrames (User Groups, etc.)
        
        Returns:
            Dictionary of transformed relationship DataFrames
        """
        try:
            # Create lookup dictionaries for each entity
            lookups = {}
            for tab in ['Users', 'Groups', 'Roles', 'Resources']:
                if tab in entity_data:
                    lookups[tab] = {}
                    key_fields = {
                        'Users': ['user_id', 'username', 'email'],
                        'Groups': ['group_id', 'group_name'],
                        'Roles': ['role_id', 'role_name'],
                        'Resources': ['resource_id', 'resource_name']
                    }
                    
                    for key in key_fields.get(tab, []):
                        if key in entity_data[tab].columns:
                            # Create a dictionary mapping each value to its row
                            lookups[tab][key] = {
                                str(row[key]): row 
                                for _, row in entity_data[tab].iterrows() 
                                if pd.notnull(row[key])
                            }

            transformed = {}
            for tab_name, df in relationship_data.items():
                if df.empty:
                    transformed[tab_name] = df
                    continue

                logger.info(f"Processing {tab_name}")
                
                # Create a copy to avoid modifying the original
                processed_df = df.copy()
                
                # Extract user_id from dictionary if needed
                if 'user_id' in processed_df.columns and isinstance(processed_df['user_id'].iloc[0], dict):
                    processed_df['user_id'] = processed_df['user_id'].apply(
                        lambda x: x.get('user_id') if isinstance(x, dict) else x
                    )

                # Handle specific relationship tables
                if tab_name == "User Groups":
                    if 'user_id' in processed_df.columns:
                        processed_df['user_id'] = processed_df['user_id'].apply(
                            lambda x: lookups['Users'].get('user_id', {}).get(str(x), {}).get('user_id', x)
                        )
                    if 'group_id' in processed_df.columns:
                        processed_df['group_id'] = processed_df['group_id'].apply(
                            lambda x: lookups['Groups'].get('group_id', {}).get(str(x), {}).get('group_id', x)
                        )
                
                # Add similar handling for other relationship tables as needed
                transformed[tab_name] = processed_df

            return transformed
        
        except Exception as e:
            logger.error(f"Error processing {tab_name}: {str(e)}")
            logger.error(f"DataFrame state at error: {df}")
            raise

    def _transform_users_tab(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform Users tab with strict schema rule compliance."""
        self.logger.info(f"Starting Users transformation with columns: {df.columns.tolist()}")
        self.logger.info(f"Mapping being applied: {mapping}")
        
        try:
            # Create a new DataFrame with mapped columns
            final_df = df.rename(columns=mapping).copy()
            
            # Log the columns after mapping
            self.logger.info(f"Columns after mapping: {final_df.columns.tolist()}")
            
            # Initialize all required columns first
            required_columns = [
                'user_id', 'username', 'email', 'first_name', 'last_name', 
                'full_name', 'is_active', 'created_at', 'updated_at', 'last_login_at'
            ]
            
            # Create missing columns
            for col in required_columns:
                if col not in final_df.columns:
                    final_df[col] = None

            # Copy email to username when username is empty
            username_mask = final_df['username'].isna() & final_df['email'].notna()
            final_df.loc[username_mask, 'username'] = final_df.loc[username_mask, 'email']

            # Extract names from email when first_name and last_name are empty
            email_mask = (
                final_df['email'].notna() & 
                final_df['first_name'].isna() & 
                final_df['last_name'].isna()
            )
            
            def extract_name_from_email(email):
                if not isinstance(email, str) or '@' not in email:
                    return None, None
                
                name_part = email.split('@')[0]
                # Handle different email formats
                if '_' in name_part:
                    parts = name_part.split('_')
                elif '.' in name_part:
                    parts = name_part.split('.')
                else:
                    return name_part, None
                
                if len(parts) >= 2:
                    # Properly capitalize names
                    first = parts[0].capitalize()
                    last = parts[1].capitalize()
                    return first, last
                return parts[0].capitalize(), None

            # Extract and set first_name and last_name from email only if they're empty
            for idx in final_df[email_mask].index:
                first, last = extract_name_from_email(final_df.loc[idx, 'email'])
                if final_df.loc[idx, 'first_name'] is None:
                    final_df.loc[idx, 'first_name'] = first
                if final_df.loc[idx, 'last_name'] is None:
                    final_df.loc[idx, 'last_name'] = last

            # Clean up name fields
            for field in ['first_name', 'last_name']:
                if field in final_df.columns:
                    final_df[field] = final_df[field].apply(
                        lambda x: x.replace('_', ' ').strip() if isinstance(x, str) else x
                    )

            # Generate full_name from first_name and last_name
            name_mask = final_df['first_name'].notna()
            final_df.loc[name_mask, 'full_name'] = final_df.loc[name_mask].apply(
                lambda row: ' '.join(filter(None, [row['first_name'], row['last_name']])), 
                axis=1
            )

            # Handle user_id according to schema rules
            missing_user_id_mask = final_df['user_id'].isna()
            if missing_user_id_mask.any():
                username_mask = missing_user_id_mask & final_df['username'].notna()
                final_df.loc[username_mask, 'user_id'] = final_df.loc[username_mask, 'username']
                
                still_missing_mask = final_df['user_id'].isna()
                email_mask = still_missing_mask & final_df['email'].notna()
                final_df.loc[email_mask, 'user_id'] = final_df.loc[email_mask, 'email']

            # Convert boolean is_active to Yes/No
            if 'is_active' in final_df.columns:
                final_df['is_active'] = final_df['is_active'].map({True: 'Yes', False: 'No'})
                final_df['is_active'] = final_df['is_active'].fillna('No')

            # Format datetime fields
            datetime_fields = ['created_at', 'updated_at', 'last_login_at']
            for field in datetime_fields:
                if field in final_df.columns and not final_df[field].isna().all():
                    final_df[field] = pd.to_datetime(final_df[field]).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Log the state of the name fields
            self.logger.info(f"Name fields null counts:")
            self.logger.info(f"  first_name: {final_df['first_name'].isna().sum()}")
            self.logger.info(f"  last_name: {final_df['last_name'].isna().sum()}")
            self.logger.info(f"  full_name: {final_df['full_name'].isna().sum()}")
            
            return final_df[required_columns]

        except Exception as e:
            self.logger.error(f"Error in Users transformation: {str(e)}")
            self.logger.error(f"Current columns: {final_df.columns.tolist() if 'final_df' in locals() else 'N/A'}")
            raise

    def _create_entity_lookups(self, entity_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Create lookup dictionaries for entity resolution."""
        lookups = {}
        for entity_type in ['Users', 'Groups', 'Roles', 'Resources']:
            if entity_type in entity_data:
                df = entity_data[entity_type]
                id_column = f"{entity_type.lower()[:-1]}_id"
                if id_column in df.columns:
                    # Convert all IDs to strings for consistent lookup
                    lookups[entity_type] = {
                        id_column: {
                            str(row[id_column]): row.to_dict() 
                            for _, row in df.iterrows() 
                            if pd.notna(row[id_column])
                        }
                    }
        return lookups

    def _log_missing_columns(self, tab_name: str, df: pd.DataFrame) -> None:
        """Log warnings for missing required columns."""
        required_columns = {
            "User Groups": ['user_id', 'group_id'],
            "User Roles": ['user_id', 'role_id'],
            "Group Roles": ['group_id', 'role_id'],
            "User Resources": ['user_id', 'resource_id'],
            "Role Resources": ['role_id', 'resource_id'],
            "Group Groups": ['parent_group_id', 'child_group_id']
        }
        
        if tab_name in required_columns:
            missing = [col for col in required_columns[tab_name] if col not in df.columns]
            if missing:
                logger.warning(f"{tab_name} is missing required columns: {missing}")

    def _get_required_columns(self, tab_name: str) -> list:
        """Get required columns for a given tab based on schema.

        Args:
            tab_name: Name of the tab/entity to get required columns for

        Returns:
            List of required column names
        """
        if tab_name not in self.schema:
            logger.warning(f"No schema defined for tab: {tab_name}")
            return []
        
        # Get all columns that are marked as mandatory in the schema
        required_cols = [
            col_name for col_name, col_def in self.schema[tab_name].items()
            if col_def.get('mandatory', False)
        ]
        
        # Special handling for relationship tables
        relationship_required_columns = {
            "User Groups": ['user_id', 'group_id'],
            "User Roles": ['user_id', 'role_id'],
            "Group Roles": ['group_id', 'role_id'],
            "User Resources": ['user_id', 'resource_id'],
            "Role Resources": ['role_id', 'resource_id'],
            "Group Resources": ['group_id', 'resource_id'],
            "Group Groups": ['parent_group_id', 'child_group_id']
        }
        
        if tab_name in relationship_required_columns:
            required_cols.extend(
                col for col in relationship_required_columns[tab_name]
                if col not in required_cols
            )
        
        return required_cols
