import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import hashlib
from colorama import Fore, Style

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, schema_file: str):
        self.schema_file = schema_file
        self.logger = logging.getLogger(__name__)
        self.group_id_map = {}  # Initialize the group_id_map
        self.transformed_data = {}  # Initialize transformed_data as empty dict
        self.role_id_map = {}  # Initialize the role_id_map
        
        with open(schema_file) as f:
            self.schema = json.load(f)

    def transform_data(self, data: Dict[str, pd.DataFrame], mappings: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]:
        """Transform all data according to schema rules."""
        transformed_data = {}
        
        # First transform Users if present
        if 'Users' in data:
            print("  Processing Users...")
            users_df = self._transform_users(data['Users'])
            if users_df is not None:
                transformed_data['Users'] = users_df
                
                # Create Roles
                print("  Processing Roles...")
                roles_df = self._transform_roles(data.get('Roles', pd.DataFrame()))
                transformed_data['Roles'] = roles_df
                
                # Create User Roles relationships
                print("  Processing User Roles...")
                user_roles_df = self._create_user_roles(users_df)
                transformed_data['User Roles'] = user_roles_df
        
        # Process other tabs using mappings
        for tab_name, df in data.items():
            if tab_name not in ['Users', 'Roles', 'User Roles']:  # Skip already processed tabs
                print(f"  Processing {tab_name}...")
                if tab_name in mappings:
                    transformed_df = self.transform_data_tab(df, tab_name, mappings[tab_name])
                    if transformed_df is not None:
                        transformed_data[tab_name] = transformed_df
        
        return transformed_data

    def _process_tab(self, df: pd.DataFrame, tab_name: str, transformed_data: Dict, mappings: Dict[str, str]):
        """Process a single tab through the transformation pipeline."""
        print(f"\n\033[1;33m► PROCESSING SIGNAL: {tab_name}")
        print(f"  RECORDS DETECTED: {df.shape[0]}\033[0m")
        
        # Clear any existing mappings for this tab
        if tab_name in self.transformed_data:
            del self.transformed_data[tab_name]
        
        transformed_df = self.transform_data_tab(df, tab_name, mappings)
        if transformed_df is not None:
            transformed_data[tab_name] = transformed_df
            self.transformed_data[tab_name] = transformed_df
            print(f"\n\033[1;32m► SIGNAL {tab_name} SUCCESSFULLY TRANSFORMED\033[0m")

    def transform_data_tab(self, df: pd.DataFrame, tab_name: str, mappings: Dict[str, str]) -> Optional[pd.DataFrame]:
        """Transform a single data tab according to mappings."""
        try:
            if tab_name in self.transformed_data:
                return self.transformed_data[tab_name]
            
            if not mappings:
                print(f"\n{Fore.RED}► ERROR: No mappings found for {tab_name}{Style.RESET_ALL}")
                return None

            print(f"\n{Fore.CYAN}► TRANSFORMING SIGNAL: {tab_name}")
            print(f"  RECORDS IN TRANSMISSION: {len(df)}{Style.RESET_ALL}")
            
            transformed_data = {}
            
            if tab_name == 'Users':
                required_cols = ['user_id', 'username', 'email', 'first_name', 'last_name', 
                               'full_name', 'is_active', 'created_at', 'updated_at', 'last_login_at']
                
                # Copy mapped fields
                for source_field, target_field in mappings.items():
                    if source_field in df.columns:
                        transformed_data[target_field] = df[source_field]
                        print(f"  ▶ MAPPING: {source_field} → {target_field}")

                # Handle derived fields
                print(f"\n{Fore.CYAN}► DERIVING ADDITIONAL FIELDS{Style.RESET_ALL}")
                
                if 'email' in transformed_data:
                    if 'user_id' not in transformed_data:
                        transformed_data['user_id'] = transformed_data['email']
                        print("  ▶ DERIVED: user_id from email")
                    
                    if 'username' not in transformed_data:
                        transformed_data['username'] = transformed_data['email'].apply(lambda x: x.split('@')[0])
                        print("  ▶ DERIVED: username from email")
                
                if 'full_name' in transformed_data:
                    if 'first_name' not in transformed_data or 'last_name' not in transformed_data:
                        names_df = transformed_data['full_name'].str.split(' ', n=1, expand=True)
                        transformed_data['first_name'] = names_df[0]
                        transformed_data['last_name'] = names_df[1]
                        print("  ▶ DERIVED: first_name and last_name from full_name")

                # Initialize missing columns
                for col in required_cols:
                    if col not in transformed_data:
                        transformed_data[col] = None
                        print(f"  ▶ INITIALIZED: {col}")

                result_df = pd.DataFrame(transformed_data)
                print(f"\n{Fore.GREEN}► TRANSFORMATION COMPLETE: {len(result_df)} records processed{Style.RESET_ALL}")
                return result_df
            
            else:
                # Handle other tabs normally
                for target_field, source_field in mappings.items():
                    if source_field in df.columns:
                        transformed_data[target_field] = df[source_field]
                transformed_df = pd.DataFrame(transformed_data)
                
                # Apply specific transformations based on tab type
                if tab_name == 'Groups':
                    transformed_df = self._transform_groups(transformed_df)
                elif tab_name == 'Roles':
                    transformed_df = self._transform_roles(transformed_df)
                elif tab_name == 'Resources':
                    transformed_df = self._transform_resources(transformed_df)
            
            # Cache the transformed data
            self.transformed_data[tab_name] = transformed_df
            return transformed_df
        
        except Exception as e:
            logger.error(f"Error transforming {tab_name}: {str(e)}")
            return None

    def _transform_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Users tab data according to schema rules."""
        try:
            # Create a copy to avoid modifying the original
            result_df = df.copy()
            
            print("  • Processing name fields...")
            # Handle name fields - split full_name into first_name and last_name
            if 'full_name' in result_df.columns:
                name_parts = result_df['full_name'].str.split(n=1, expand=True)
                result_df['first_name'] = name_parts[0]
                result_df['last_name'] = name_parts[1].fillna('')
            
            print("  • Processing email fields...")
            # Handle email-based fields
            if 'email' in result_df.columns:
                if 'user_id' not in result_df.columns:
                    result_df['user_id'] = result_df['email']
                if 'username' not in result_df.columns:
                    result_df['username'] = result_df['email'].apply(lambda x: str(x).split('@')[0] if pd.notna(x) else None)
            
            print("  • Processing datetime fields...")
            # Convert datetime fields to ISO format
            datetime_fields = ['created_at', 'updated_at', 'last_login_at']
            for field in datetime_fields:
                if field in result_df.columns and result_df[field].notna().any():
                    result_df[field] = pd.to_datetime(result_df[field]).dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            print("  • Standardizing is_active field...")
            # Standardize is_active values
            if 'is_active' in result_df.columns:
                result_df['is_active'] = result_df['is_active'].map({
                    'Active': 'Yes',
                    'Deactivated': 'No',
                    'Invited': 'No'
                }).fillna('No')
            
            # Define the required column order
            required_columns = [
                'user_id',
                'username',
                'email',
                'first_name',
                'last_name',
                'full_name',
                'is_active',
                'created_at',
                'updated_at',
                'last_login_at'
            ]
            
            print("  • Ensuring all required columns exist...")
            # Ensure all required columns exist
            for col in required_columns:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # Reorder columns
            result_df = result_df[required_columns]
            
            print("  • User transformation complete")
            return result_df
        
        except Exception as e:
            print(f"  • ERROR in _transform_users: {str(e)}")
            raise

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
        """Transform Roles tab data according to schema rules."""
        COLUMN_ORDER = ['role_id', 'role_name', 'role_description']
        
        # Define default system roles
        default_roles = [
            {'role_name': 'Deactivated', 'role_description': 'Auto-generated role for Deactivated'},
            {'role_name': 'Active', 'role_description': 'Auto-generated role for Active'},
            {'role_name': 'Invited', 'role_description': 'Auto-generated role for Invited'},
            {'role_name': 'Declined', 'role_description': 'Auto-generated role for Declined'}
        ]
        
        # Create DataFrame with default roles
        roles_df = pd.DataFrame(default_roles)
        
        # Add role_id as 1-based index
        roles_df['role_id'] = range(1, len(roles_df) + 1)
        
        # Update role_id_map
        self.role_id_map = {row['role_name']: row['role_id'] 
                            for _, row in roles_df.iterrows()}
        
        logger.info(f"Created {len(self.role_id_map)} role ID mappings")
        logger.debug(f"Role mappings: {dict(list(self.role_id_map.items()))}")
        
        return roles_df[COLUMN_ORDER]

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
        
        active_values = {
            'true', '1', 'yes', 'y', 'active', 'enabled', 't', 'invited'
        }
        
        inactive_values = {
            'false', '0', 'no', 'n', 'inactive', 'disabled', 'f', 'deactivated'
        }
        
        if value_str in active_values:
            return 'Yes'
        elif value_str in inactive_values:
            return 'No'
        
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

    def save_transformed_data(self, data: Dict[str, pd.DataFrame], output_path: str) -> None:
        """Save transformed data to Excel file."""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for tab_name, df in data.items():
                    if df is not None and not df.empty:
                        # Ensure all required columns are present
                        if tab_name == 'Users':
                            required_cols = ['user_id', 'username', 'email', 'first_name', 
                                          'last_name', 'full_name', 'is_active', 
                                          'created_at', 'updated_at', 'last_login_at']
                            for col in required_cols:
                                if col not in df.columns:
                                    df[col] = None
                            df = df[required_cols]  # Reorder columns
                    
                        df.to_excel(writer, sheet_name=tab_name, index=False)
                        self.logger.info(f"Saved {len(df)} records to {tab_name} sheet")
        
            self.logger.info(f"Successfully saved transformed data to {output_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving transformed data: {str(e)}")
            return False

    def _display_field_alignments(self, df: pd.DataFrame, tab_name: str, mappings: Dict):
        """Display current field alignments with sample values."""
        print(f"\n{Fore.CYAN}> CURRENT FIELD ALIGNMENTS FOR {tab_name}")
        print("=" * 60 + f"{Style.RESET_ALL}\n")
        
        current_mappings = mappings[tab_name].get('mappings', {})
        
        for idx, column in enumerate(df.columns, 1):
            target = current_mappings.get(column, 'unmapped')
            
            # Get sample values
            sample_values = df[column].dropna().head(3).tolist()
            samples_str = f"[{', '.join(str(x) for x in sample_values)}]"
            
            # Color coding
            if target == 'unmapped':
                mapping_color = Fore.RED
            else:
                mapping_color = Fore.GREEN
            
            print(f"{Fore.WHITE}{idx}. {column} {mapping_color}→ {target}")
            print(f"   Sample values: {Fore.YELLOW}{samples_str}{Style.RESET_ALL}")

    def _display_options_menu(self):
        """Display the options menu in fixed-width format."""
        print(f"\n{Fore.CYAN}╔══════════════════ ALIGNMENT OPTIONS ══════════════════╗")
        print("║ 1-N) Select field number to modify mapping            ║")
        print("║ v) Validate mappings                                  ║")
        print("║ c) Continue with current mappings                     ║")
        print("║ r) Reset all mappings                                 ║")
        print("║ s) Skip this tab                                      ║")
        print(f"╚════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
        print(f"\n{Fore.CYAN}► ENTER COMMAND: {Style.RESET_ALL}", end='', flush=True)

    def _interactive_mapping_modification(self, df: pd.DataFrame, tab_name: str, mappings: Dict[str, Dict]) -> str:
        """Handle interactive mapping modifications."""
        while True:
            self._display_options_menu()
            command = input("\n> ENTER COMMAND: ").strip().lower()
            
            if command == 'r':
                # Reset mappings without regenerating
                mappings[tab_name]['mappings'] = {}
                mappings[tab_name]['validated'] = False
                print("\n╔════════════════════ RESET COMPLETE ═══════════════════╗")
                print("║ All mappings have been cleared                        ║")
                print("╚════════════════════════════════════════════════════════╝")
                continue
            
            elif command == 'c':
                if not mappings[tab_name]['mappings']:
                    print("\n╔════════════════════ WARNING ══════════════════════════╗")
                    print("║ No mappings defined! Please map at least one field.    ║")
                    print("╚════════════════════════════════════════════════════════╝")
                    continue
                return 'continue'
            
            elif command == 's':
                return 'skip'
            
            elif command == 'v':
                # Add validation logic here
                mappings[tab_name]['validated'] = True
                continue
            
            elif command.isdigit():
                field_num = int(command)
                if 1 <= field_num <= len(df.columns):
                    self._modify_field_mapping(df, tab_name, field_num - 1, mappings)
                else:
                    print("\nInvalid field number. Please try again.")
                continue
            
            else:
                print("\nInvalid command. Please try again.")
                continue

    def _validate_mappings(self, df: pd.DataFrame, tab_name: str, mappings: Dict, show_results: bool = False) -> bool:
        """Validate current mappings against schema rules."""
        if tab_name == "Users":
            # Initialize mappings if not present
            if tab_name not in mappings:
                mappings[tab_name] = {'mappings': {}}
            
            current_mappings = mappings[tab_name].get('mappings', {})
            
            # Auto-map missing required fields with default values
            if 'user_id' not in current_mappings:
                current_mappings['user_id'] = 'email'  # Use email as user_id
            
            if 'username' not in current_mappings:
                current_mappings['username'] = 'email'  # Use email as username
            
            if 'first_name' not in current_mappings and 'last_name' not in current_mappings:
                if 'full_name' in current_mappings:
                    # Split full_name into first_name and last_name
                    current_mappings['first_name'] = 'full_name'
                    current_mappings['last_name'] = 'full_name'
        
            # Update the mappings
            mappings[tab_name]['mappings'] = current_mappings
        
            if show_results:
                print("\n✓ Mappings configured:")
                for target, source in current_mappings.items():
                    print(f"  {source} → {target}")
        
            return True
        
        return True

    def _modify_field_mapping(self, df: pd.DataFrame, tab_name: str, field_num: int, mappings: Dict):
        """Modify mapping for a specific field."""
        columns = list(df.columns)
        current_field = columns[field_num]
        
        print(f"\n{Fore.CYAN}Modifying mapping for: {Fore.WHITE}{current_field}")
        print(f"{Fore.CYAN}Available target fields:{Style.RESET_ALL}")
        
        if tab_name == "Users":
            target_fields = ['user_id', 'username', 'email', 'first_name', 'last_name', 
                            'full_name', 'is_active', 'created_at', 'updated_at', 'last_login_at']
        else:
            target_fields = columns
        
        for idx, field in enumerate(target_fields, 1):
            print(f"{Fore.WHITE}{idx}) {field}{Style.RESET_ALL}")
        
        choice = input(f"\n{Fore.CYAN}Select target field number (or 'b' to go back): {Style.RESET_ALL}")
        if choice.isdigit() and 1 <= int(choice) <= len(target_fields):
            new_target = target_fields[int(choice) - 1]
            mappings[tab_name]['mappings'][current_field] = new_target
            print(f"\n{Fore.GREEN}Mapped {current_field} → {new_target}{Style.RESET_ALL}")

    def _reset_mappings(self, df: pd.DataFrame, tab_name: str, mappings: Dict) -> Dict:
        """Reset mappings to empty state."""
        print(f"\n{Fore.YELLOW}╔════════════════════ RESET INITIATED ═══════════════════╗")
        
        # Completely clear the mappings for this tab
        mappings[tab_name] = {
            'mappings': {},
            'validated': False
        }
        
        print(f"║ ✓ All mappings cleared                                   ║")
        print(f"╚════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
        
        return mappings

    def _display_current_mappings(self, df: pd.DataFrame, mappings: Dict[str, str]):
        """Display current mappings with sample data."""
        print(f"\n{Fore.CYAN}Current Field Alignments{Style.RESET_ALL}")
        print("=" * 60)

        for idx, (column, target) in enumerate(mappings.items(), 1):
            # Get sample values
            samples = df[column].dropna().unique()[:3].tolist()
            samples_str = ", ".join(str(s) for s in samples)
            
            # Determine mapping color based on whether it's mapped
            mapping_color = Fore.GREEN if target else Fore.RED
            target_display = target if target else "unmapped"
            
            # Display mapping with consistent formatting
            print(f"\n{Fore.WHITE}{idx}. {column} {mapping_color}→ {target_display}{Style.RESET_ALL}")
            print(f"   Sample values: {Fore.YELLOW}[{samples_str}]{Style.RESET_ALL}")

    def _modify_mapping(self, current_field: str, df: pd.DataFrame, tab_name: str, mappings: Dict[str, Dict]) -> None:
        """Handle modification of a single mapping."""
        print(f"\n{Fore.CYAN}Modifying mapping for: {current_field}{Style.RESET_ALL}")
        
        # Get available target fields
        if tab_name == 'Users':
            target_fields = ['user_id', 'username', 'email', 'first_name', 'last_name',
                            'full_name', 'is_active', 'created_at', 'updated_at', 'last_login_at']
        else:
            target_fields = list(df.columns)
        
        print(f"\n{Fore.CYAN}Available target fields:{Style.RESET_ALL}")
        for idx, field in enumerate(target_fields, 1):
            print(f"{Fore.WHITE}{idx}) {field}{Style.RESET_ALL}")
        
        print(f"\n{Fore.CYAN}Select target field number (or 'b' to go back):{Style.RESET_ALL} ", end='')
        choice = input().strip().lower()
        
        if choice == 'b':
            return
        
        if choice.isdigit() and 1 <= int(choice) <= len(target_fields):
            new_target = target_fields[int(choice) - 1]
            mappings[tab_name]['mappings'][current_field] = new_target
            print(f"\n{Fore.GREEN}✓ Mapped {current_field} → {new_target}{Style.RESET_ALL}")
            input("\nPress Enter to continue...")

    def _extract_roles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract roles from the dataset."""
        # Extract unique roles from is_active field if it contains role information
        roles = []
        if 'is_active' in df.columns:
            roles.extend(df['is_active'].unique())
        
        # Create roles DataFrame
        roles_df = pd.DataFrame({
            'role_id': roles,
            'role_name': roles,
            'role_description': [f"Auto-generated role for {role}" for role in roles]
        })
        
        return roles_df

    def _create_user_roles(self, users_df: pd.DataFrame) -> pd.DataFrame:
        """Create user-role relationships based on user status."""
        user_roles = []
        
        for _, user in users_df.iterrows():
            status = str(user.get('is_active', '')).strip()
            user_id = user['user_id']
            
            # Map Yes/No to Active/Deactivated
            if status.lower() == 'yes':
                role_name = 'Active'
            elif status.lower() == 'no':
                role_name = 'Deactivated'
            else:
                # Handle other statuses (Invited, Declined) if present
                role_name = status if status in self.role_id_map else 'Deactivated'
            
            role_id = self.role_id_map.get(role_name)
            if role_id is not None:
                user_roles.append({
                    'user_id': user_id,
                    'role_id': role_id
                })
            else:
                logger.warning(f"No role_id found for status: {status}")
        
        return pd.DataFrame(user_roles)
