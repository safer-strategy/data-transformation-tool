
import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import hashlib

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, schema_file: str):
        self.schema_file = schema_file
        self.logger = logging.getLogger(__name__)
        self.group_id_map = {}  # Initialize the group_id_map
        self.transformed_data = {}  # Initialize transformed_data as empty dict
        
        with open(schema_file) as f:
            self.schema = json.load(f)

    def transform_data(self, data: Dict[str, pd.DataFrame], mappings: Dict[str, Dict]) -> Dict[str, pd.DataFrame]:
        """Transform all data according to mappings."""
        transformed_data = {}
        
        # Transform Users if present
        if 'Users' in data:
            df = data['Users']
            transformed_df = self.transform_data_tab(df, mappings, 'Users')
            if transformed_df is not None:
                transformed_data['Users'] = transformed_df
        
        # Transform Groups if present
        if 'Groups' in data:
            df = data['Groups']
            transformed_df = self.transform_data_tab(df, mappings, 'Groups')
            if transformed_df is not None:
                transformed_data['Groups'] = transformed_df
        
        # Transform User Groups only if both Users and Groups are present
        if 'User Groups' in data and 'Users' in transformed_data and 'Groups' in transformed_data:
            transformed_df = self.transform_data_tab(data['User Groups'], mappings, 'User Groups')
            if transformed_df is not None and not transformed_df.empty:
                transformed_data['User Groups'] = transformed_df
        
        # Transform remaining tabs
        for tab_name, df in data.items():
            if tab_name not in ['Users', 'Groups', 'User Groups']:
                transformed_df = self.transform_data_tab(df, mappings, tab_name)
                if transformed_df is not None:
                    transformed_data[tab_name] = transformed_df
        
        return transformed_data

    def transform_data_tab(self, df: pd.DataFrame, mappings: Dict[str, Dict], tab_name: str) -> Optional[pd.DataFrame]:
        """Transform a single data tab according to mappings."""
        try:
            # Get the mappings for this tab
            tab_mappings = mappings.get(tab_name, {}).get('mappings', {})
            if not tab_mappings:
                logger.warning(f"No mappings found for tab {tab_name}")
                return None

            # Create transformed DataFrame with mapped columns
            transformed_data = {}
            for target_field, source_field in tab_mappings.items():
                if source_field in df.columns:
                    transformed_data[target_field] = df[source_field]

            transformed_df = pd.DataFrame(transformed_data)

            # Apply specific transformations based on tab type
            if tab_name == 'Users':
                transformed_df = self._transform_users(transformed_df)
            elif tab_name == 'Groups':
                transformed_df = self._transform_groups(transformed_df)
            elif tab_name == 'Roles':
                transformed_df = self._transform_roles(transformed_df)
            elif tab_name == 'Resources':
                transformed_df = self._transform_resources(transformed_df)
            
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error transforming data for tab {tab_name}: {str(e)}")
            return None

    def _transform_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Users tab data."""
        # Ensure all required columns exist
        required_cols = ['user_id', 'username', 'email', 'first_name', 'last_name', 'full_name', 'is_active']
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

        # Transform is_active field - check all possible column names
        status_columns = ['Active', 'Status', 'IsActive', 'is_active', 'active']
        status_column = next((col for col in status_columns if col in df.columns), None)
        
        if status_column:
            self.logger.info(f"Found status column: {status_column}")
            self.logger.info(f"Unique status values before transformation: {df[status_column].unique().tolist()}")
            df['is_active'] = df[status_column].apply(self._transform_boolean_to_yes_no)
            self.logger.info(f"Unique is_active values after transformation: {df['is_active'].unique().tolist()}")
        else:
            self.logger.warning("No status column found, defaulting is_active to 'No'")
            df['is_active'] = 'No'

        # Convert datetime fields
        date_fields = ['created_at', 'updated_at', 'last_login_at']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        return df

    def _transform_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Groups tab data according to schema rules."""
        COLUMN_ORDER = ['group_id', 'group_name', 'group_description']
        
        if df.empty:
            return pd.DataFrame(columns=COLUMN_ORDER)

        # Create a clean DataFrame with just the columns we need
        clean_df = pd.DataFrame({
            'group_name': df['group_name'] if 'group_name' in df.columns else df['Name'],
            'group_description': df['group_description'] if 'group_description' in df.columns 
                               else df.get('Description', None)
        })
        
        # Remove any rows where group_name is empty or NaN
        clean_df = clean_df.dropna(subset=['group_name'])
        clean_df = clean_df[clean_df['group_name'].str.strip() != '']
        
        # Remove any duplicates
        clean_df = clean_df.drop_duplicates(subset=['group_name'])
        
        # Generate sequential group_ids
        clean_df['group_id'] = range(1, len(clean_df) + 1)
        
        # Reset and create the group_id_map
        self.group_id_map.clear()  # Clear existing mappings
        
        # Store mapping for relationship resolution
        for _, row in clean_df.iterrows():
            group_name = str(row['group_name']).strip()
            if group_name:
                self.group_id_map[group_name] = row['group_id']
        
        logger.info(f"Created {len(self.group_id_map)} group ID mappings")
        logger.debug(f"First 5 group mappings: {dict(list(self.group_id_map.items())[:5])}")
        
        # Fill missing descriptions without using inplace
        clean_df = clean_df.assign(
            group_description=lambda x: x['group_description'].fillna(x['group_name'])
        )
        
        return clean_df[COLUMN_ORDER].copy()

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
        """Transform various boolean/status values to 'Yes' or 'No'."""
        if pd.isna(value):
            return 'No'
        
        value_str = str(value).lower().strip()
        
        # Values that mean "Yes"
        active_values = {
            'true', '1', 'yes', 'y', 'active', 'enabled', 't',
            'invited'  # Adding 'invited' as an active status
        }
        
        # Values that mean "No"
        inactive_values = {
            'false', '0', 'no', 'n', 'inactive', 'disabled', 'f',
            'deactivated'  # Adding 'deactivated' as an inactive status
        }
        
        self.logger.debug(f"Processing status value: '{value}'")
        
        if value_str in active_values:
            self.logger.debug(f"Matched active value: '{value}' → 'Yes'")
            return 'Yes'
        elif value_str in inactive_values:
            self.logger.debug(f"Matched inactive value: '{value}' → 'No'")
            return 'No'
        
        # Log unmatched values
        self.logger.warning(f"Unmatched status value '{value}' defaulting to 'No'")
        return 'No'

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

    def _transform_user_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform User Groups relationships using username and group_name."""
        logger.info(f"Processing User Groups relationships from {len(df)} records")
        logger.debug(f"Input columns: {df.columns.tolist()}")
        
        # Create mappings from Users table
        if 'Users' in self.transformed_data:
            users_df = self.transformed_data['Users']
            user_mappings = {}
            
            # Build comprehensive user mapping
            for _, user in users_df.iterrows():
                user_id = user.get('user_id')
                username = user.get('username')
                email = user.get('email')
                
                if user_id:
                    if username: user_mappings[username] = user_id
                    if email: user_mappings[email] = user_id
                elif username and email:
                    user_mappings[username] = email
                    user_mappings[email] = email
                elif username:
                    user_mappings[username] = username
                elif email:
                    user_mappings[email] = email
            
            logger.info(f"Created user mappings for {len(user_mappings)} identifiers")
        else:
            logger.error("Users table not found in transformed data")
            return pd.DataFrame(columns=['user_id', 'group_id'])

        # Create the relationships
        relationships = []
        for _, row in df.iterrows():
            # Try multiple possible column names for user identifier
            user_identifier = None
            for col in ['user_id', 'username', 'email', 'User ID', 'Username', 'Email']:
                if col in row and pd.notna(row[col]):
                    user_identifier = str(row[col]).strip()
                    break
                    
            # Try multiple possible column names for group
            group_name = None
            for col in ['group_name', 'group', 'Group', 'Group Name']:
                if col in row and pd.notna(row[col]):
                    group_name = str(row[col]).strip()
                    break

            if user_identifier and group_name:
                user_id = user_mappings.get(user_identifier, user_identifier)
                group_id = self.group_id_map.get(group_name)
                
                if group_id:  # We always need a valid group_id
                    relationships.append({
                        'user_id': user_id,
                        'group_id': group_id
                    })
                else:
                    logger.warning(f"Could not find group_id for group_name: {group_name}")

        # Create final DataFrame and remove duplicates
        result_df = pd.DataFrame(relationships)
        if not result_df.empty:
            result_df = result_df.drop_duplicates()
            logger.info(f"Created {len(result_df)} unique user-group relationships")
        else:
            logger.warning("No valid user-group relationships created")
            result_df = pd.DataFrame(columns=['user_id', 'group_id'])
        
        return result_df

    def organize_flattened_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Process data into separate sheets according to schema rules."""
        organized_data = {}
        
        # Process Users
        if 'Users' in data:
            users_df = data['Users'].copy()
            column_mapping = {
                'User ID': 'user_id',
                'First name': 'first_name',
                'Last name': 'last_name',
                'Email': 'email',
                'Active': 'is_active',
                'Created': 'created_at',
                'Updated': 'updated_at',
                'Last login': 'last_login_at'
            }
            users_df.rename(columns=column_mapping, inplace=True)
            organized_data['Users'] = self._transform_users(users_df)
            logging.info(f"Processed Users data: {len(users_df)} records")

        # Process Groups first to build the group_id_map
        if 'Groups' in data:
            groups_df = data['Groups'].copy()
            groups_df.rename(columns={
                'Name': 'group_name',
                'Description': 'group_description'
            }, inplace=True)
            organized_data['Groups'] = self._transform_groups(groups_df)
            logging.info(f"Processed Groups data: {len(groups_df)} records")

        # Process User Groups directly from the input sheet
        if 'User Groups' in data and self.group_id_map:
            user_groups_df = data['User Groups'].copy()
            user_group_pairs = []
            
            # Log the input data for debugging
            logging.info(f"Processing User Groups sheet with columns: {user_groups_df.columns.tolist()}")
            logging.info(f"First few rows of User Groups:\n{user_groups_df.head()}")
            
            for _, row in user_groups_df.iterrows():
                user_id = str(row['User ID']).strip()
                group_name = str(row['Group']).strip()
                
                if group_name in self.group_id_map:
                    user_group_pairs.append({
                        'user_id': user_id,
                        'group_id': self.group_id_map[group_name]
                    })
                else:
                    logging.warning(f"Group not found in mapping: {group_name}")
            
            if user_group_pairs:
                organized_data['User Groups'] = pd.DataFrame(user_group_pairs).drop_duplicates()
                logging.info(f"Created {len(organized_data['User Groups'])} user-group relationships")
            else:
                logging.warning("No valid user-group relationships found")
                organized_data['User Groups'] = pd.DataFrame(columns=['user_id', 'group_id'])

        # Log final state
        for name, df in organized_data.items():
            logging.info(f"{name} shape: {df.shape}, columns: {df.columns.tolist()}")
            if not df.empty:
                logging.debug(f"{name} first few rows:\n{df.head()}")

        return organized_data
