import pandas as pd
from pathlib import Path
import logging
from typing import Dict, List
import os
from data_transformer import DataTransformer

logger = logging.getLogger(__name__)

class OutputGenerator:
    def __init__(self, schema_path: str):
        self.schema_path = schema_path
        self.MAX_ROWS = 1000000  # Slightly under Excel's limit for safety
        self.logger = logging.getLogger(__name__)
        
    def organize_data_by_schema(self, flattened_df: pd.DataFrame, transformer: 'DataTransformer') -> Dict[str, pd.DataFrame]:
        """Organize flattened data into schema-defined sheets."""
        organized_data = {}
        
        # Add detailed logging of input data
        logger.info(f"Initial flattened_df columns: {flattened_df.columns.tolist()}")
        logger.info(f"Initial flattened_df shape: {flattened_df.shape}")
        
        # Log potential group-related columns
        group_related_cols = [col for col in flattened_df.columns 
                             if any(term in col.lower() 
                                   for term in ['group', 'grp', 'team', 'role', 'member'])]
        logger.info(f"Found potential group-related columns: {group_related_cols}")
        
        # Sample the data from these columns
        for col in group_related_cols:
            sample_values = flattened_df[col].dropna().head(5).tolist()
            logger.info(f"Sample values from {col}: {sample_values}")

        # Users tab
        users_df = pd.DataFrame()
        if not flattened_df.empty:
            # Extract user fields according to schema rules
            user_fields = {
                'user_id': ['user_id', 'userid', 'uid'],
                'username': ['username', 'user_name', 'login'],
                'email': ['email', 'mail', 'email_address'],
                'first_name': ['first_name', 'firstname'],
                'last_name': ['last_name', 'lastname'],
                'full_name': ['full_name', 'fullname', 'name'],
                'is_active': ['is_active', 'active', 'status'],
                'updated_at': ['updated_at', 'updated', 'last_updated'],
                'last_login_at': ['last_login_at', 'last_login', 'lastlogin']
            }
            
            # Map and extract user fields
            user_data = {}
            for target_field, possible_fields in user_fields.items():
                for field in possible_fields:
                    if field in flattened_df.columns:
                        user_data[target_field] = flattened_df[field]
                        break
            
            if user_data:
                users_df = pd.DataFrame(user_data)
                # Transform is_active to Yes/No
                if 'is_active' in users_df.columns:
                    users_df['is_active'] = users_df['is_active'].apply(
                        lambda x: 'Yes' if str(x).lower() in {'true', '1', 'yes', 'y', 'active', 'enabled', 't'} else 'No'
                    )
                # Transform datetime fields
                for date_field in ['updated_at', 'last_login_at']:
                    if date_field in users_df.columns:
                        users_df[date_field] = pd.to_datetime(users_df[date_field], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                organized_data['Users'] = users_df
                logger.info(f"Created Users DataFrame with shape: {users_df.shape}")

        # Groups tab - Process this FIRST to establish group_id_map
        groups_df = pd.DataFrame()
        if 'group_name' in flattened_df.columns:
            # Create a mapping of unique groups
            groups_df = flattened_df[['group_name']].drop_duplicates()
            groups_df = groups_df[groups_df['group_name'].notna()]
            
            if 'group_description' in flattened_df.columns:
                groups_df['group_description'] = flattened_df['group_description']
            
            # Transform groups using the transformer to establish group_id_map
            organized_data['Groups'] = transformer._transform_groups(groups_df)
            logger.info(f"Processed Groups data: {len(groups_df)} records")

        # User Groups relationships
        if 'Users' in organized_data and 'Groups' in organized_data:
            logger.info("Processing User Groups relationships")
            
            # Store the current state in transformer
            transformer.transformed_data = {
                'Users': organized_data['Users'],
                'Groups': organized_data['Groups']
            }
            
            # First attempt: Try direct relationships
            valid_rows = flattened_df[flattened_df['user_id'].notna() & flattened_df['group_name'].notna()]
            
            if not valid_rows.empty:
                logger.info(f"Found {len(valid_rows)} rows with both user_id and group_name")
                relationships = []
                
                for _, row in valid_rows.iterrows():
                    user_id = str(row['user_id']).strip()
                    group_name = str(row['group_name']).strip()
                    
                    if group_name in transformer.group_id_map:
                        relationships.append({
                            'user_id': user_id,
                            'group_id': transformer.group_id_map[group_name]
                        })
            
                if relationships:
                    user_groups_df = pd.DataFrame(relationships).drop_duplicates()
                    organized_data['User Groups'] = user_groups_df
                    logger.info(f"Created {len(user_groups_df)} direct user-group relationships")
                else:
                    logger.info("No direct relationships found, attempting transformation")
                    user_groups_df = transformer._transform_user_groups(flattened_df)
                    if not user_groups_df.empty:
                        organized_data['User Groups'] = user_groups_df
                        logger.info(f"Created {len(user_groups_df)} transformed user-group relationships")
                    else:
                        logger.warning("No user-group relationships could be created")
                        organized_data['User Groups'] = pd.DataFrame(columns=['user_id', 'group_id'])
            else:
                logger.info("No rows with both user_id and group_name found, attempting transformation")
                user_groups_df = transformer._transform_user_groups(flattened_df)
                if not user_groups_df.empty:
                    organized_data['User Groups'] = user_groups_df
                    logger.info(f"Created {len(user_groups_df)} transformed user-group relationships")
                else:
                    logger.warning("No user-group relationships could be created")
                    organized_data['User Groups'] = pd.DataFrame(columns=['user_id', 'group_id'])

        return organized_data

    def generate_excel(self, data: Dict[str, pd.DataFrame], output_path: str) -> None:
        """Generate Excel output with chunking for large sheets."""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, df in data.items():
                    if len(df) > self.MAX_ROWS:
                        # Split into chunks
                        chunk_size = self.MAX_ROWS
                        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
                        
                        # Write each chunk to a separate sheet
                        for i, chunk in enumerate(chunks, 1):
                            chunk_name = f"{sheet_name}_{i}"
                            chunk.to_excel(writer, sheet_name=chunk_name, index=False)
                    else:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        
        except Exception as e:
            logging.error(f"Error generating output: {str(e)}")
            raise
