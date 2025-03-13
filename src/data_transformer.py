import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, schema_path: str = 'schema.json'):
        self.group_id_map = {}
        self.next_group_id = 1
        
        # Load schema to know mandatory screens and columns
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
        
        # Define mandatory screens
        self.mandatory_screens = [
            "Users", "Groups", "Roles", "Resources",
            "User Groups", "User Roles", "Group Roles",
            "User Resources", "Role Resources"
        ]

    def transform_data(self, input_data: Dict[str, pd.DataFrame], mappings: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]:
        """Transform input data and ensure all mandatory screens and columns exist."""
        transformed = {}
        
        try:
            # First, ensure all mandatory screens exist
            for screen in self.mandatory_screens:
                if screen not in input_data:
                    logger.info(f"Creating missing mandatory screen: {screen}")
                    input_data[screen] = pd.DataFrame()
            
            # Process Groups tab first
            if 'Groups' in input_data:
                transformed['Groups'] = self._transform_groups_tab(
                    input_data['Groups'],
                    mappings.get('Groups', {})
                )
            
            # Process remaining tabs
            for tab_name, df in input_data.items():
                if tab_name == 'Groups':
                    continue  # Already processed
                
                logger.info(f"Processing tab: {tab_name}")
                
                if tab_name == "Users":
                    transformed[tab_name] = self._transform_users_tab(
                        df,
                        mappings.get(tab_name, {})
                    )
                else:
                    transformed[tab_name] = self._transform_generic_tab(
                        df,
                        tab_name,
                        mappings.get(tab_name, {})
                    )
            
            # Ensure all mandatory screens exist in transformed data
            for screen in self.mandatory_screens:
                if screen not in transformed:
                    logger.info(f"Creating missing mandatory screen in output: {screen}")
                    transformed[screen] = self._create_empty_tab(screen)
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error in transform_data: {str(e)}")
            logger.error("Current state of transformed data:")
            for tab_name, df in transformed.items():
                logger.error(f"{tab_name} shape: {df.shape}")
                logger.error(f"{tab_name} columns: {df.columns.tolist()}")
            raise

    def _create_empty_tab(self, tab_name: str) -> pd.DataFrame:
        """Create an empty DataFrame with required columns for a given tab."""
        if tab_name not in self.schema:
            logger.warning(f"No schema found for tab: {tab_name}")
            return pd.DataFrame()
        
        # Get all columns from schema
        columns = list(self.schema[tab_name].keys())
        return pd.DataFrame(columns=columns)

    def _transform_generic_tab(self, df: pd.DataFrame, tab_name: str, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform any tab ensuring all required columns exist."""
        try:
            # Start with empty DataFrame if input is empty
            if df.empty:
                return self._create_empty_tab(tab_name)
            
            # Apply column mapping
            mapped_df = df.rename(columns=mapping)
            
            # Ensure all required columns exist
            if tab_name in self.schema:
                for column in self.schema[tab_name].keys():
                    if column not in mapped_df.columns:
                        mapped_df[column] = None
                        logger.debug(f"Added missing column {column} to {tab_name}")
                
                # Reorder columns according to schema
                mapped_df = mapped_df.reindex(columns=list(self.schema[tab_name].keys()))
            
            return mapped_df
            
        except Exception as e:
            logger.error(f"Error in _transform_generic_tab for {tab_name}: {str(e)}")
            logger.error(f"Input columns: {df.columns.tolist()}")
            logger.error(f"Mapping: {mapping}")
            raise

    def _transform_users_tab(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """Transform Users tab ensuring all required columns exist."""
        try:
            # Handle duplicate mappings by prioritizing required fields
            reverse_mapping = {}
            duplicate_targets = {}
            
            # First pass: identify duplicates
            for source, target in mapping.items():
                if target in reverse_mapping:
                    if target not in duplicate_targets:
                        duplicate_targets[target] = [(reverse_mapping[target], source)]
                    else:
                        duplicate_targets[target].append((reverse_mapping[target], source))
                else:
                    reverse_mapping[target] = source
            
            # Resolve duplicates by checking schema requirements
            for target, sources in duplicate_targets.items():
                if target in self.schema.get("Users", {}):
                    is_required = self.schema["Users"][target].get("mandatory", False)
                    if is_required:
                        # Find the source with non-null values
                        non_null_sources = [
                            source for source_pair in sources 
                            for source in source_pair 
                            if df[source].notna().any()
                        ]
                        if non_null_sources:
                            # Keep the first non-null source
                            reverse_mapping[target] = non_null_sources[0]
                            logger.info(f"Keeping required field '{target}' mapped from '{non_null_sources[0]}' (has data)")
                        else:
                            # Keep the first source if no non-null values found
                            reverse_mapping[target] = sources[0][0]
                            logger.info(f"Keeping required field '{target}' mapped from '{sources[0][0]}' (all null)")
                    else:
                        # For non-required fields, keep the first non-null source or drop if all null
                        non_null_sources = [
                            source for source_pair in sources 
                            for source in source_pair 
                            if df[source].notna().any()
                        ]
                        if non_null_sources:
                            reverse_mapping[target] = non_null_sources[0]
                            logger.info(f"Keeping optional field '{target}' mapped from '{non_null_sources[0]}' (has data)")
                        else:
                            # Remove mapping if all sources are null
                            reverse_mapping.pop(target, None)
                            logger.info(f"Dropping optional field '{target}' as all sources are null")
            
            # Create a new DataFrame with correct columns
            result_df = pd.DataFrame()
            
            # Map columns using the resolved reverse mapping
            for target_col, source_col in reverse_mapping.items():
                if source_col in df.columns:
                    result_df[target_col] = df[source_col]
            
            # Ensure all required columns exist
            required_columns = list(self.schema["Users"].keys())
            for col in required_columns:
                if col not in result_df.columns:
                    result_df[col] = None
                    logger.debug(f"Added missing column {col} to Users tab")
            
            # Handle datetime fields
            date_columns = ['created_at', 'updated_at', 'last_login_at']
            for col in date_columns:
                if col in result_df.columns:
                    result_df[col] = pd.to_datetime(result_df[col], errors='coerce')
                    result_df[col] = result_df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # Handle boolean fields
            bool_columns = ['is_active']
            for col in bool_columns:
                if col in result_df.columns:
                    result_df[col] = result_df[col].map({'Yes': True, 'No': False})
            
            return result_df.reindex(columns=required_columns)
            
        except Exception as e:
            logger.error(f"Error in _transform_users_tab: {str(e)}")
            logger.error(f"Available columns: {df.columns.tolist()}")
            logger.error(f"Mapping: {mapping}")
            raise

    def _transform_groups_tab(
        self,
        df: pd.DataFrame,
        mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """Transform Groups tab ensuring all required columns exist.

        Args:
            df: Input DataFrame containing groups data
            mapping: Dictionary mapping source columns to target columns

        Returns:
            pd.DataFrame: Transformed groups DataFrame with required columns

        Raises:
            ValueError: If group_name column is missing when needed for group_id generation
        """
        try:
            # If input is empty, return empty DataFrame with required columns
            if df.empty:
                return self._create_empty_tab("Groups")
            
            # Apply column mapping
            mapped_df = df.rename(columns=mapping)
            result_df = pd.DataFrame()
            
            # Handle auto-generation of group_id if needed
            if "__generated_group_id__" in mapping:
                if 'group_name' in mapped_df.columns:
                    result_df['group_name'] = mapped_df['group_name']
                    result_df['group_id'] = mapped_df.apply(
                        lambda row: self._get_or_create_group_id(row['group_name']),
                        axis=1
                    )
                else:
                    logger.error("Cannot generate group_id: group_name column not found")
                    raise ValueError(
                        "group_name column is required for group_id generation"
                    )
            else:
                # Copy all mapped columns
                for col in mapped_df.columns:
                    result_df[col] = mapped_df[col]
            
            # Ensure all required columns exist
            required_columns = list(self.schema["Groups"].keys())
            for col in required_columns:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # Reorder columns according to schema
            return result_df.reindex(columns=required_columns)
            
        except Exception as e:
            logger.error(f"Error in _transform_groups_tab: {str(e)}")
            logger.error(f"Input columns: {df.columns.tolist()}")
            logger.error(f"Mapping: {mapping}")
            raise

    def _get_or_create_group_id(self, group_name: str) -> str:
        """Get existing group ID or create new one."""
        if pd.isna(group_name):
            return None
        
        group_name = str(group_name).strip()
        if group_name not in self.group_id_map:
            self.group_id_map[group_name] = f"G{self.next_group_id:06d}"
            self.next_group_id += 1
        return self.group_id_map[group_name]

    def _transform_user_groups_tab(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform User Groups tab using the generated group IDs."""
        result_df = df.copy()
        
        # Log available columns
        logger.debug(f"User Groups columns before transform: {result_df.columns.tolist()}")
        
        # Check for required columns
        if 'group_id' not in result_df.columns:
            logger.warning("group_id column missing in User Groups tab")
            # Add empty group_id column if missing
            result_df['group_id'] = pd.NA
        
        if 'user_id' not in result_df.columns:
            logger.warning("user_id column missing in User Groups tab")
            # Add empty user_id column if missing
            result_df['user_id'] = pd.NA
        
        # Apply transformations only on non-null values
        if 'group_id' in result_df.columns:
            result_df['group_id'] = result_df['group_id'].apply(
                lambda x: self.group_id_map.get(str(x).strip(), x) if pd.notna(x) else pd.NA
            )
        
        logger.debug(f"User Groups columns after transform: {result_df.columns.tolist()}")
        return result_df

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
        try:
            # First apply the column mapping
            result_df = df.rename(columns=mapping)
            
            # Create missing columns if they don't exist
            required_columns = ['user_id', 'username', 'email', 'first_name', 'last_name', 'is_active']
            for col in required_columns:
                if col not in result_df.columns:
                    result_df[col] = None
                    logger.debug(f"Created missing column: {col}")

            # Ensure at least one identifier is present
            has_identifier = result_df[['user_id', 'username', 'email']].notna().any(axis=1)
            if not has_identifier.all():
                missing_ids = result_df[~has_identifier].index
                logger.error(f"Records missing required identifiers at indices: {missing_ids.tolist()}")
                raise ValueError("Each user record must have at least one identifier")

            # Handle datetime fields
            date_columns = ['created_at', 'updated_at', 'last_login_at']
            for col in date_columns:
                if col in result_df.columns:
                    result_df[col] = pd.to_datetime(result_df[col], errors='coerce')
                    result_df[col] = result_df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Handle is_active field
            if 'is_active' in result_df.columns:
                result_df['is_active'] = result_df['is_active'].map({
                    True: 'Yes', False: 'No',
                    'True': 'Yes', 'False': 'No',
                    'YES': 'Yes', 'NO': 'No',
                    'Y': 'Yes', 'N': 'No',
                    1: 'Yes', 0: 'No'
                }).fillna('No')

            return result_df

        except Exception as e:
            logger.error(f"Error in _transform_users_tab: {str(e)}")
            logger.error(f"Available columns: {df.columns.tolist()}")
            logger.error(f"Mapping: {mapping}")
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
