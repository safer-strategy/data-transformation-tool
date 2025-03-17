import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import pandas as pd
from colorama import Fore, Style, init

# Initialize colorama
init()

# Update the import statement to be explicit
from header_mapper import HeaderMapper
from data_transformer import DataTransformer
from validator import Validator
from output_generator import OutputGenerator

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def print_debug_info(df: pd.DataFrame, msg: str = ""):
    """Print debug information about the DataFrame."""
    print(f"\nDEBUG {msg}")
    print("Columns:", df.columns.tolist())
    print("First few rows:")
    print(df.head())
    print("-" * 80)


def validate_input_file(file_path: str) -> bool:
    """
    Validate input file extension and existence.
    
    Args:
        file_path: Path to the input file
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    path = Path(file_path)
    if not path.exists():
        logging.error(f"File not found: {file_path}")
        return False
    
    if path.suffix.lower() not in ['.xlsx', '.csv']:
        logging.error(
            f"Unsupported file type: {path.suffix}. "
            "Only .xlsx and .csv files are supported."
        )
        return False
    
    return True


def read_excel_sheets(file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Read Excel sheets with intelligent preview and selection.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Optional[Dict[str, pd.DataFrame]]: Dictionary of sheet names and their data,
        or None if reading fails
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        if len(sheet_names) == 0:
            logging.error("Excel file contains no sheets")
            return None
        
        if len(sheet_names) == 1:
            return {sheet_names[0]: pd.read_excel(file_path)}
        
        print("\nAvailable sheets with preview:")
        print("=" * 80)
        
        sheet_previews = {}
        for idx, name in enumerate(sheet_names, 1):
            df = pd.read_excel(file_path, sheet_name=name, nrows=5)  # Read first 5 rows for preview
            if not df.empty:
                print(f"\n{idx}) {name}")
                print("-" * 40)
                print("Columns:")
                for col in df.columns:
                    samples = df[col].dropna().unique()[:3].tolist()
                    samples_str = ", ".join(str(s) for s in samples)
                    print(f"  • {col:<30} Samples: {samples_str}")
                sheet_previews[name] = df
            else:
                print(f"\n{idx}) {name} (Empty)")
        
        while True:
            print("\nOptions:")
            print("1) Select specific sheets")
            print("2) Use all non-empty sheets (Recommended)")
            choice = input("\nChoose option (1-2): ").strip()
            
            if choice == "1":
                return select_specific_sheets(file_path, sheet_names)
            elif choice == "2":
                return {name: pd.read_excel(file_path, sheet_name=name) 
                       for name, preview_df in sheet_previews.items() 
                       if not preview_df.empty}
            else:
                print("Invalid choice. Please try again.")
        
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        return None

def select_specific_sheets(file_path: str, sheet_names: List[str]) -> Dict[str, pd.DataFrame]:
    """Handle specific sheet selection with validation."""
    while True:
        choice = input(
            "\nSelect sheets to process (comma-separated numbers): "
        ).strip()
        
        try:
            indices = [int(x.strip()) for x in choice.split(',')]
            selected_sheets = [
                sheet_names[i-1] for i in indices 
                if 1 <= i <= len(sheet_names)
            ]
            if selected_sheets:
                return {
                    name: pd.read_excel(file_path, sheet_name=name) 
                    for name in selected_sheets
                }
            print("No valid sheets selected. Please try again.")
        except (ValueError, IndexError):
            print("Invalid selection. Please try again.")

def print_processing_info(sheet_name: str, target_name: str, columns: List[str]):
    """Print processing information with color."""
    print(f"\n{Fore.CYAN}Processing sheet: {Fore.GREEN}{sheet_name} {Fore.WHITE}-> {Fore.YELLOW}{target_name}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Available columns: {Fore.WHITE}{columns}{Style.RESET_ALL}")

def print_mappings_preview(mappings: Dict[str, Dict[str, Dict]], data: Dict[str, pd.DataFrame]):
    """Display proposed mappings in a colored YAML-like format with data samples."""
    print(f"\n{Fore.CYAN}Proposed Mappings Preview:{Style.RESET_ALL}")
    print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)

    for tab_name, tab_mappings in mappings.items():
        print(f"\n{Fore.GREEN}{tab_name}:{Style.RESET_ALL}")
        for target_field, source_info in tab_mappings.items():
            sheet_name = source_info['sheet']
            source_header = source_info['header']
            df = data[sheet_name]
            
            # Get sample values
            samples = df[source_header].dropna().unique()[:3].tolist()
            samples_str = ", ".join(str(s) for s in samples)
            
            print(f"  {Fore.YELLOW}{target_field}: {Style.RESET_ALL}")
            print(f"    {Fore.WHITE}source: {Fore.CYAN}{sheet_name}.{source_header}{Style.RESET_ALL}")
            print(f"    {Fore.WHITE}samples: {Fore.MAGENTA}[{samples_str}]{Style.RESET_ALL}")

def preview_mappings(mappings: Dict[str, Dict[str, str]], data: Dict[str, pd.DataFrame], schema_path: str) -> Dict[str, pd.DataFrame]:
    """Display proposed mappings and handle transformation."""
    print_mappings_preview(mappings, data)
    
    print(f"\n{Fore.CYAN}Options:{Style.RESET_ALL}")
    print(f"{Fore.WHITE}1) Accept all mappings")
    print(f"2) Modify specific mapping{Style.RESET_ALL}")
    
    while True:
        choice = input("\nChoose option (1-2): ").strip()
        
        if choice == "1":
            # Transform data for each tab
            transformed_data = {}
            transformer = DataTransformer(schema_path)
            
            for tab_name, tab_mappings in mappings.items():
                if not tab_mappings:
                    continue
                
                # Get the source sheet name from the mappings
                source_sheets = {info['sheet'] for info in tab_mappings.values()}
                if len(source_sheets) != 1:
                    logging.warning(f"Inconsistent source sheets for tab {tab_name}")
                    continue
                
                source_sheet = source_sheets.pop()
                if source_sheet not in data:
                    logging.warning(f"Source sheet {source_sheet} not found in data")
                    continue
                
                source_df = data[source_sheet]
                transformed_df = transformer.transform_data_tab(
                    df=source_df,
                    mappings=tab_mappings,
                    tab_name=tab_name  # Pass the tab_name parameter
                )
                
                if transformed_df is not None:
                    transformed_data[tab_name] = transformed_df
            
            return transformed_data
            
        elif choice == "2":
            modify_mapping(mappings, data)
        else:
            print(f"{Fore.RED}Invalid choice. Please enter 1 or 2.{Style.RESET_ALL}")

def modify_mapping(mappings: Dict[str, Dict[str, str]], data: Dict[str, pd.DataFrame]) -> None:
    """
    Allow user to modify specific mapping with intelligent suggestions.
    
    Args:
        mappings: Dictionary of tab names to their header mappings
        data: Dictionary of sheet names to their DataFrames
    """
    while True:
        # Show available tabs
        print("\nAvailable output tabs:")
        print("=" * 80)
        tabs = list(mappings.keys())
        for idx, tab in enumerate(tabs, 1):
            field_count = len(mappings[tab])
            print(f"{idx}) {tab} ({field_count} fields)")
        print("0) Return to main menu")
        
        try:
            tab_choice = input("\nSelect tab number (0 to return): ").strip()
            if tab_choice == "0":
                return
            
            tab_idx = int(tab_choice) - 1
            if not (0 <= tab_idx < len(tabs)):
                print("Invalid tab selection")
                continue
            
            tab_name = tabs[tab_idx]
            tab_mappings = mappings[tab_name]
            
            # Show fields in selected tab
            print(f"\nFields in {tab_name}:")
            print("=" * 80)
            fields = list(tab_mappings.keys())
            for idx, field in enumerate(fields, 1):
                current = tab_mappings[field]
                current_sheet = current['sheet']
                current_header = current['header']
                
                # Get sample values for current mapping
                samples = []
                if current_sheet in data and current_header in data[current_sheet].columns:
                    samples = data[current_sheet][current_header].dropna().unique()[:3].tolist()
                samples_str = ", ".join(str(s) for s in samples)
                
                print(f"{idx}) {field}")
                print(f"   Current: {current_sheet}.{current_header}")
                print(f"   Samples: [{samples_str}]")
            
            print("0) Return to tab selection")
            
            field_choice = input("\nSelect field number (0 to return): ").strip()
            if field_choice == "0":
                continue
                
            field_idx = int(field_choice) - 1
            if not (0 <= field_idx < len(fields)):
                print("Invalid field selection")
                continue
            
            field_name = fields[field_idx]
            
            # Show available source sheets
            print("\nAvailable source sheets:")
            print("=" * 80)
            sheets = list(data.keys())
            for idx, sheet in enumerate(sheets, 1):
                col_count = len(data[sheet].columns)
                print(f"{idx}) {sheet} ({col_count} columns)")
            
            sheet_choice = int(input("\nSelect source sheet number: ")) - 1
            if not (0 <= sheet_choice < len(sheets)):
                print("Invalid sheet selection")
                continue
            
            selected_sheet = sheets[sheet_choice]
            
            # Show available columns with fuzzy matching suggestions
            print(f"\nAvailable columns in {selected_sheet}:")
            print("=" * 80)
            columns = list(data[selected_sheet].columns)
            
            # Sort columns by similarity to field name for better suggestions
            from fuzzywuzzy import fuzz
            scored_columns = [
                (col, fuzz.ratio(col.lower(), field_name.lower()))
                for col in columns
            ]
            scored_columns.sort(key=lambda x: x[1], reverse=True)
            
            for idx, (col, score) in enumerate(scored_columns, 1):
                samples = data[selected_sheet][col].dropna().unique()[:3].tolist()
                samples_str = ", ".join(str(s) for s in samples)
                match_str = f"(Match: {score}%)" if score > 50 else ""
                print(f"{idx}) {col} {match_str}")
                print(f"   Samples: [{samples_str}]")
            
            col_choice = int(input("\nSelect column number: ")) - 1
            if not (0 <= col_choice < len(columns)):
                print("Invalid column selection")
                continue
            
            selected_column = scored_columns[col_choice][0]
            
            # Update the mapping
            mappings[tab_name][field_name] = {
                'sheet': selected_sheet,
                'header': selected_column
            }
            
            print(f"\n✓ Updated mapping for {field_name}:")
            print(f"   {selected_sheet}.{selected_column}")
            
            # Show preview of new mapping
            samples = data[selected_sheet][selected_column].dropna().unique()[:3].tolist()
            samples_str = ", ".join(str(s) for s in samples)
            print(f"   Sample values: [{samples_str}]")
            
            continue_choice = input("\nModify another mapping? (y/n): ").lower().strip()
            if continue_choice != 'y':
                return
                
        except (ValueError, IndexError) as e:
            print(f"Invalid input: {str(e)}")
            continue


def setup_logging(debug_mode: bool = False) -> None:
    """
    Configure logging settings.
    
    Args:
        debug_mode: Enable debug logging if True
    """
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(message)s',
        handlers=[
            logging.FileHandler('validation.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    if not debug_mode:
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('PIL').setLevel(logging.WARNING)


def print_header(text: str) -> None:
    """
    Print a formatted header.
    
    Args:
        text: Header text to display
    """
    print(f"\n{'='*80}\n{text.center(80)}\n{'='*80}\n")


def get_output_path(input_path: str) -> str:
    """
    Generate output path based on input path.
    
    Args:
        input_path: Path to the input file
        
    Returns:
        str: Path where output file should be saved
    """
    input_path = Path(input_path)
    converts_dir = Path("converts")
    converts_dir.mkdir(exist_ok=True)
    
    output_name = f"converted_{input_path.stem}.xlsx"
    return str(converts_dir / output_name)


def flatten_sheets(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Flatten multiple sheets into a single DataFrame while preserving relationships."""
    flattened_dfs = []
    
    logging.info(f"Starting sheet flattening with sheets: {list(data.keys())}")
    
    # Process Users sheet first if it exists
    if 'Users' in data:
        users_df = data['Users'].copy()
        logging.info(f"Users sheet columns: {users_df.columns.tolist()}")
        flattened_dfs.append(users_df)
        logging.info(f"Added Users data: {len(users_df)} records")

    # Process Groups sheet independently
    if 'Groups' in data:
        groups_df = data['Groups'].copy()
        logging.info(f"Groups sheet columns: {groups_df.columns.tolist()}")
        flattened_dfs.append(groups_df)
        logging.info(f"Added Groups data: {len(groups_df)} records")

    # Look for membership data in all sheets
    for sheet_name, df in data.items():
        if sheet_name not in ['Users', 'Groups']:
            logging.info(f"Processing sheet {sheet_name} for relationships")
            logging.info(f"Sheet columns: {df.columns.tolist()}")
            
            # Look for columns indicating user-group relationships
            user_cols = [col for col in df.columns if any(term in col.lower() for term in ['user', 'member', 'userid'])]
            group_cols = [col for col in df.columns if 'group' in col.lower()]
            
            logging.info(f"Found user columns: {user_cols}")
            logging.info(f"Found group columns: {group_cols}")
            
            if user_cols and group_cols:
                relationship_df = df[user_cols + group_cols].copy()
                # Rename columns to standard names if they're different
                column_mapping = {}
                for col in user_cols:
                    if col.lower() != 'user_id':
                        column_mapping[col] = 'user_id'
                for col in group_cols:
                    if col.lower() != 'group':
                        column_mapping[col] = 'Group'
                if column_mapping:
                    relationship_df = relationship_df.rename(columns=column_mapping)
                
                logging.info(f"Found relationship data in {sheet_name}: {len(relationship_df)} records")
                logging.debug(f"Sample relationships:\n{relationship_df.head()}")
                flattened_dfs.append(relationship_df)
            else:
                logging.warning(f"Sheet {sheet_name} doesn't contain user-group relationship data")

    # Combine all DataFrames
    if not flattened_dfs:
        logging.error("No data to flatten")
        return pd.DataFrame()

    flattened_df = pd.concat(flattened_dfs, axis=0, ignore_index=True)
    logging.info(f"Final flattened data shape: {flattened_df.shape}")
    logging.info(f"Final columns: {flattened_df.columns.tolist()}")
    logging.debug(f"Sample of flattened data:\n{flattened_df.head()}")
    
    return flattened_df


def print_banner():
    """Print the application banner."""
    print("\n╔════════════════════════════════════════╗")
    print("║          Linx AppMapper               ║")
    print("╚════════════════════════════════════════╝\n")


def organize_flattened_data(flattened_df: pd.DataFrame, transformer) -> Dict[str, pd.DataFrame]:
    """Process flattened data into separate sheets according to schema rules."""
    logging.info(f"Starting data organization with {len(flattened_df)} records")
    logging.info(f"Available columns: {flattened_df.columns.tolist()}")
    organized_data = {}
    
    # Users tab
    available_user_fields = [
        field for field in [
            'user_id', 'username', 'email', 'first_name', 'last_name', 
            'full_name', 'is_active', 'created_at', 'updated_at', 'last_login_at'
        ] if field in flattened_df.columns
    ]
    
    if available_user_fields:
        users_df = flattened_df[available_user_fields].drop_duplicates()
        organized_data['Users'] = transformer._transform_users(users_df)
        logging.info(f"Processed Users data: {len(users_df)} records")
    
    # Groups tab - Process this BEFORE User Groups
    if 'group_name' in flattened_df.columns:
        groups_df = flattened_df[['group_name']].drop_duplicates()
        groups_df = groups_df[groups_df['group_name'].notna()]
        if 'group_description' in flattened_df.columns:
            groups_df['group_description'] = flattened_df['group_description']
        organized_data['Groups'] = transformer._transform_groups(groups_df)
        logging.info(f"Processed Groups data: {len(groups_df)} records")
    
    # User Groups tab - Process after Groups to ensure we have group_id_map
    membership_columns = [col for col in flattened_df.columns if any(term in col.lower() for term in ['group', 'member', 'user'])]
    logging.info(f"Found potential membership columns: {membership_columns}")
    
    if membership_columns:
        # Look for direct membership data
        user_group_pairs = []
        
        # Try different column combinations for user-group relationships
        user_indicators = ['user', 'member']
        group_indicators = ['group']
        
        for user_col in [col for col in membership_columns if any(ind in col.lower() for ind in user_indicators)]:
            for group_col in [col for col in membership_columns if any(ind in col.lower() for ind in group_indicators)]:
                logging.info(f"Checking relationship between {user_col} and {group_col}")
                
                valid_pairs = flattened_df[[user_col, group_col]].dropna()
                if not valid_pairs.empty:
                    logging.info(f"Found {len(valid_pairs)} potential relationships")
                    logging.debug(f"Sample relationships:\n{valid_pairs.head()}")
                    
                    for _, row in valid_pairs.iterrows():
                        user_id = str(row[user_col]).strip()
                        group_name = str(row[group_col]).strip()
                        
                        if group_name in transformer.group_id_map:
                            user_group_pairs.append({
                                'user_id': user_id,
                                'group_id': transformer.group_id_map[group_name]
                            })
        
        if user_group_pairs:
            user_groups_df = pd.DataFrame(user_group_pairs).drop_duplicates()
            organized_data['User Groups'] = user_groups_df
            logging.info(f"Created {len(user_groups_df)} user-group relationships")
            logging.debug(f"Sample of final relationships:\n{user_groups_df.head()}")
        else:
            logging.warning("No valid user-group relationships found")
            organized_data['User Groups'] = pd.DataFrame(columns=['user_id', 'group_id'])

    # Log the final state of the data
    for name, df in organized_data.items():
        logging.info(f"{name} shape: {df.shape}, columns: {df.columns.tolist()}")
        if not df.empty:
            logging.debug(f"{name} first few rows:\n{df.head()}")

    return organized_data


def analyze_data_size(df: pd.DataFrame, name: str) -> None:
    """Analyze and log data size information."""
    print(f"\nAnalyzing {name}:")
    print(f"Total rows: {len(df):,}")
    if 'user_id' in df.columns:
        unique_users = df['user_id'].nunique()
        print(f"Unique users: {unique_users:,}")
    if 'group_id' in df.columns:
        unique_groups = df['group_id'].nunique()
        print(f"Unique groups: {unique_groups:,}")
    if 'user_id' in df.columns and 'group_id' in df.columns:
        print(f"Average groups per user: {len(df) / df['user_id'].nunique():.2f}")


def main(input_path: str) -> int:
    try:
        print_banner()
        
        print("▶ Setting up Python environment")
        
        # Add debug logging for schema path
        schema_path = Path("src/schema.json")
        print(f"Looking for schema at: {schema_path.absolute()}")
        if not schema_path.exists():
            logging.error(f"Schema file not found at {schema_path.absolute()}")
            return 1
            
        print(f"Schema file found: {schema_path}")
        
        if not validate_input_file(input_path):
            return 1

        # Initialize components
        print("Initializing components...")
        mapper = HeaderMapper(str(schema_path))
        transformer = DataTransformer(str(schema_path))  # Add this line
        validator = Validator(str(schema_path))          # Add this line if not already present
        
        # Read input based on file type
        input_file = Path(input_path)
        print(f"Reading input file: {input_file}")
        if input_file.suffix.lower() == '.xlsx':
            data = read_excel_sheets(input_path)
        else:  # .csv
            data = {"Sheet1": pd.read_csv(input_path)}
        
        if not data:
            return 1

        print("Generating mappings...")
        try:
            proposed_mappings = {}
            
            # Map sheets to schema tabs
            sheet_to_tab_mapping = {
                'Users': 'Users',
                'Groups': 'Groups',
                'Roles': 'Roles',
                'Resources': 'Resources',
                'User Groups': 'UserGroups',
                'User Roles': 'UserRoles',
                'Group Roles': 'GroupRoles',
                'User Resources': 'UserResources',
                'Role Resources': 'RoleResources',
                'Group Resources': 'GroupResources',
                'Group Groups': 'GroupGroups'
            }
            
            for sheet_name, df in data.items():
                if df.empty:
                    continue
                    
                # Get corresponding tab name from mapping
                tab_name = sheet_to_tab_mapping.get(sheet_name)
                if not tab_name:
                    print(f"Warning: No schema mapping found for sheet {sheet_name}")
                    continue
                
                print_processing_info(sheet_name, tab_name, list(df.columns))
                
                headers = list(df.columns)
                sheet_mappings = mapper.map_headers(headers, tab_name)
                
                # Debug output
                print(f"Received mappings from map_headers: {sheet_mappings}")
                
                if sheet_mappings:
                    # Review mappings with the user
                    reviewed_mappings = mapper.review_mappings(
                        sheet_mappings,
                        headers,
                        tab_name,
                        df
                    )
                    
                    if reviewed_mappings:
                        proposed_mappings[tab_name] = {
                            field: {'sheet': sheet_name, 'header': header}
                            for header, field in reviewed_mappings.items()
                        }
                        print(f"Added mappings for {tab_name}: {proposed_mappings[tab_name]}")
                    else:
                        print(f"No mappings confirmed for {tab_name}")
                else:
                    print(f"No initial mappings generated for {tab_name}")
            
            if not proposed_mappings:
                print("\nNo mappings were generated. Please check the schema and input data.")
                return 1
                
            # Display mapping preview
            preview_mappings(proposed_mappings, data, str(schema_path))
            
            # Apply approved mappings using the transformer
            transformed_data = {}
            for tab_name, tab_mappings in proposed_mappings.items():
                # Get the source sheet name from any of the mappings
                if not tab_mappings:
                    continue
                first_mapping = next(iter(tab_mappings.values()))
                source_sheet = first_mapping['sheet']
                
                if source_sheet not in data:
                    logging.warning(f"Source sheet {source_sheet} not found in data")
                    continue
                    
                source_df = data[source_sheet]
                transformed_df = transformer.transform_data_tab(
                    df=source_df,
                    mappings=tab_mappings,
                    tab_name=tab_name  # Add the missing parameter
                )
                if transformed_df is not None:
                    transformed_data[tab_name] = transformed_df
            
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return 1

        # Flatten data if multiple sheets exist
        if len(transformed_data) > 1:
            print("\nFlattening multiple sheets...")
            flattened_data = flatten_sheets(transformed_data)
            
            if flattened_data is not None:
                print("DEBUG: Flattened data columns:", flattened_data.columns.tolist())
                output_generator = OutputGenerator(schema_path)
                organized_data = output_generator.organize_data_by_schema(flattened_data, transformer)
                
                if organized_data:
                    # Debug output for each processed DataFrame
                    for tab_name, df in organized_data.items():
                        print(f"DEBUG: Processed {tab_name} columns:", df.columns.tolist())
                    transformed_data = organized_data  # or {"Flattened": organized_data} if needed
                else:
                    print("Failed to process flattened data")
                    return 1
            else:
                print("Failed to flatten data")
                return 1
        
        # Validate and generate output
        print("\nValidating data...")
        validator = Validator(schema_path)
        valid_data = {}
        
        for sheet_name, df in transformed_data.items():
            print(f"\nProcessing sheet: {sheet_name}")
            print(f"Columns: {df.columns.tolist()}")
            print(f"Shape: {df.shape}")
            
            # Store the validated data
            valid_data[sheet_name] = df
        
        if not valid_data:
            print("No valid data to process")
            return 1
            
        # Generate output files
        output_path = get_output_path(input_path)
        generator = OutputGenerator(schema_path)
        
        # Debug print before generating output
        print("\nData to be written:")
        for sheet_name, df in valid_data.items():
            print(f"{sheet_name}: {len(df)} rows, {df.columns.tolist()}")
        
        generator.generate_excel(valid_data, output_path)
        print(f"\nValid records written to: {output_path}")
        
        print("\n✓ Processing completed successfully!")
        return 0

    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # If no argument provided, check the uploads directory
        uploads_dir = Path("uploads")
        if uploads_dir.exists():
            # Get all Excel and CSV files
            files = list(uploads_dir.glob("*.xlsx")) + list(uploads_dir.glob("*.csv"))
            if files:
                # Use the first file found
                input_path = str(files[0])
                sys.exit(main(input_path))
            else:
                logging.error("No Excel or CSV files found in uploads directory")
                sys.exit(1)
        else:
            logging.error("No input file provided and uploads directory not found")
            sys.exit(1)
    else:
        # Use the provided command line argument
        sys.exit(main(sys.argv[1]))
