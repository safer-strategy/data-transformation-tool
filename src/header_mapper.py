import json
import os
from pathlib import Path
import logging
from fuzzywuzzy import process
from typing import List, Dict, Tuple, Set
import pandas as pd
from difflib import SequenceMatcher
import yaml
from colorama import Fore, Style, init

# Initialize colorama
init()

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(text: str):
    """Print a formatted header."""
    print(f"\n{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(Fore.CYAN + "=" * len(text) + Style.RESET_ALL)

def print_mapping(target: str, source: str, is_unmapped: bool = False):
    """Print a single mapping with color."""
    source_color = Fore.YELLOW if is_unmapped else Fore.GREEN
    print(f"{Fore.WHITE}'{target}' ← {source_color}'{source}'{Style.RESET_ALL}")

logger = logging.getLogger(__name__)

class HeaderMapper:
    def __init__(self, schema_file: str, auto_mapping_enabled: bool = True):
        self.schema_file = schema_file
        self.logger = logging.getLogger(__name__)
        self.auto_mapping_enabled = auto_mapping_enabled
        
        # Load the schema
        try:
            with open(schema_file) as f:
                self.schema = json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading schema: {e}")
            raise
        
        # Load the YAML mappings
        current_dir = Path(schema_file).parent
        yaml_path = current_dir / 'header_mappings.yaml'
        try:
            with open(yaml_path) as f:
                self.yaml_mappings = yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading YAML mappings: {e}")
            raise

        self.saved_mappings = {}
        self.mappings_file = Path(schema_file).parent / 'mappings_history.json'
        self.load_saved_mappings()

    def load_saved_mappings(self):
        """Load previously saved mappings."""
        self.saved_mappings = {}
        if self.mappings_file.exists():
            try:
                with open(self.mappings_file, 'r') as f:
                    self.saved_mappings = json.load(f)
            except json.JSONDecodeError as e:
                self.logger.error(f"Error loading mappings file: {e}")
                self.saved_mappings = {}

    def save_mappings(self, mappings: Dict[str, str]) -> None:
        """Save mappings including explicitly skipped headers."""
        try:
            # Update saved_mappings with new mappings
            self.saved_mappings.update(mappings)
            
            # Write to file
            with open(self.mappings_file, 'w') as f:
                json.dump(self.saved_mappings, f, indent=2)
            self.logger.info(f"Mappings saved successfully to {self.mappings_file}")
        except Exception as e:
            self.logger.error(f"Error saving mappings: {e}")

    def map_headers(self, headers: List[str], tab_name: str) -> Dict[str, str]:
        """Map input headers to schema fields."""
        if not self.auto_mapping_enabled:
            return {}  # Return empty dictionary when auto-mapping is disabled
        
        tab_schema = self.schema.get(tab_name)
        if not tab_schema:
            # Special handling for User Groups tab
            if tab_name.lower().replace(" ", "") == "usergroups":
                # Use predefined mappings for User Groups
                mappings = {
                    'User ID': 'user_id',
                    'Group': 'group_id'  # This will be transformed to actual group_id later
                }
                return {header: target for header, target in mappings.items() 
                       if header in headers}
            else:
                self.logger.warning(f"No schema found for tab {tab_name}")
                return {}
        
        mappings = {}
        
        # Get schema for this tab
        tab_schema = self.schema.get(tab_name, {})
        if not tab_schema:
            self.logger.warning(f"No schema found for tab {tab_name}")
            return mappings
            
        self.logger.debug(f"Processing {len(headers)} headers for {tab_name}")
        
        # Helper function to normalize strings for comparison
        def normalize(s: str) -> str:
            return s.lower().replace(' ', '_').replace('-', '_')
        
        # First try exact matches and saved mappings
        for header in headers:
            # Check saved mappings first
            mapping_key = f"{tab_name}:{header}"
            if mapping_key in self.saved_mappings:
                mappings[header] = self.saved_mappings[mapping_key]
                continue
                
            normalized_header = normalize(header)
            matched = False
            
            # Try exact match with schema fields
            for field, props in tab_schema.items():
                if normalize(field) == normalized_header:
                    mappings[header] = field
                    matched = True
                    break
                    
                # Try synonyms
                if not matched:
                    synonyms = props.get('synonyms', [])
                    if any(normalize(syn) == normalized_header for syn in synonyms):
                        mappings[header] = field
                        matched = True
                        break
        
        # Try fuzzy matching for remaining unmapped headers
        unmapped_headers = [h for h in headers if h not in mappings]
        for header in unmapped_headers:
            best_match = None
            best_score = 60  # Minimum match score threshold
            normalized_header = normalize(header)
            
            for field, props in tab_schema.items():
                # Skip if field is already mapped
                if field in mappings.values():
                    continue
                    
                # Calculate match scores including normalized comparison
                field_score = self.calculate_match_score(normalized_header, normalize(field))
                synonym_scores = [
                    self.calculate_match_score(normalized_header, normalize(syn))
                    for syn in props.get('synonyms', [])
                ]
                
                max_score = max([field_score] + synonym_scores)
                if max_score > best_score:
                    best_score = max_score
                    best_match = field
            
            if best_match:
                mappings[header] = best_match
        
        self.logger.info(f"Generated mappings for {tab_name}: {mappings}")
        return mappings

    def _get_prominent_fields(self, tab_name: str) -> List[str]:
        """Get list of prominent fields for a tab."""
        prominent = {
            "Users": ["user_id", "username", "email", "first_name", "last_name"],
            "Groups": ["group_id", "group_name"],
            "Roles": ["role_id", "role_name"],
            "Resources": ["resource_id", "resource_name"],
            "User Groups": ["user_id", "group_id"],
            "User Roles": ["user_id", "role_id"],
            "Group Roles": ["group_id", "role_id"],
            "User Resources": ["user_id", "resource_id"],
            "Role Resources": ["role_id", "resource_id"],
            "Group Resources": ["group_id", "resource_id"],
            "Group Groups": ["parent_group_id", "child_group_id"]
        }
        return prominent.get(tab_name, [])

    def get_fuzzy_matches(self, header: str, tab_schema: Dict, num_matches: int = 5) -> List[Tuple[str, int]]:
        """Get fuzzy matches for a header from valid schema fields only."""
        # Only include fields that are in the schema
        valid_fields = list(tab_schema.keys())
        
        # Get matches for the header against valid fields
        matches = process.extract(header.lower(), valid_fields, limit=len(valid_fields))
        
        # Filter and sort matches
        final_matches = [(field, score) for field, score in matches if field in valid_fields]
        final_matches.sort(key=lambda x: x[1], reverse=True)
        
        return final_matches[:num_matches]

    def print_separator(self, char: str = '-', length: int = 60):
        """Print a separator line."""
        print(char * length)

    def review_mappings(
        self,
        mappings: Dict[str, str],
        input_headers: List[str],
        tab_name: str,
        preview_data: pd.DataFrame
    ) -> Dict[str, str]:
        """Review and confirm mappings with focus on mandatory fields first."""
        tab_schema = self.schema.get(tab_name, {})
        valid_fields = sorted(tab_schema.keys())

        while True:
            clear_screen()
            # Use the new print_mappings_preview function
            self.print_mappings_preview({k: v for k, v in mappings.items()}, {tab_name: preview_data})

            choice = input().lower().strip()
            
            if choice == 'c':
                # Validate mandatory fields are mapped before continuing
                mandatory_fields = self.get_mandatory_fields(tab_name)
                unmapped_mandatory = [field for field in mandatory_fields 
                                    if field not in mappings.values()]
                
                if unmapped_mandatory:
                    print(f"\n{Fore.RED}Warning: The following mandatory fields are unmapped:{Style.RESET_ALL}")
                    for field in unmapped_mandatory:
                        print(f"- {field}")
                    print("\nPlease map these fields before continuing.")
                    input("\nPress Enter to continue...")
                    continue
                break
            elif choice == 's':
                return self.start_over_mapping(input_headers, tab_name, preview_data)
            else:
                try:
                    idx = int(choice)
                    if 1 <= idx <= len(valid_fields):
                        target_field = valid_fields[idx-1]
                        current_source = next((k for k, v in mappings.items() if v == target_field), "(unmapped)")
                        
                        print(f"\nModifying mapping for: '{target_field}'")
                        print(f"Currently mapped to: '{current_source}'")
                        
                        # Show available source attributes with sample values
                        print("\nAvailable source attributes:")
                        for src_idx, header in enumerate(input_headers, 1):
                            mapped_to = mappings.get(header, "")
                            status = f" (currently mapped to '{mapped_to}')" if mapped_to else ""
                            if header in preview_data.columns:
                                sample_values = preview_data[header].dropna().unique()[:3].tolist()
                                print(f"{src_idx}) '{header}'{status}")
                                print(f"   Sample values: {sample_values}")
                            else:
                                print(f"{src_idx}) '{header}'{status}")
                        
                        print("0) Remove mapping")
                        
                        src_choice = input("\nChoose source attribute (0-N): ")
                        try:
                            src_idx = int(src_choice)
                            if src_idx == 0:
                                # Remove any existing mapping to this target
                                for k, v in list(mappings.items()):
                                    if v == target_field:
                                        del mappings[k]
                            elif 1 <= src_idx <= len(input_headers):
                                source_header = input_headers[src_idx-1]
                                # Remove any existing mapping to this target
                                for k, v in list(mappings.items()):
                                    if v == target_field:
                                        del mappings[k]
                                # Remove any existing mapping from this source
                                if source_header in mappings:
                                    del mappings[source_header]
                                mappings[source_header] = target_field
                                print(f"\n✓ Updated mapping: '{target_field}' ← '{source_header}'")
                        except ValueError:
                            print("Invalid choice, keeping current mapping")
                            
                except ValueError:
                    print("Invalid choice, please try again")

        # Save confirmed mappings
        for header, target in mappings.items():
            if target:
                mapping_key = f"{tab_name}:{header}"
                self.saved_mappings[mapping_key] = target

        self.save_mappings(self.saved_mappings)
        return mappings

    def calculate_match_score(self, source: str, target: str) -> int:
        """Calculate match score between source and target strings."""
        return int(SequenceMatcher(None, source.lower(), target.lower()).ratio() * 100)

    def _confirm_mappings(self, mappings: Dict[str, str], tab_name: str, 
                         preview_data: pd.DataFrame) -> Dict[str, str]:
        """Show mapping preview with sample data and confirm."""
        print(f"\nProposed mappings for {tab_name}:")
        print("\nYAML format:")
        preview_mappings = {tab_name: mappings}
        print(yaml.dump(preview_mappings, default_flow_style=False))

        print("\nData preview:")
        for source, target in mappings.items():
            if target:  # Only show mapped fields
                print(f"\n{source} → {target}")
                print("Sample values:", preview_data[source].head().tolist())

        while True:
            confirm = input("\nAccept these mappings? (y/n): ").lower()
            if confirm == 'y':
                # Save confirmed mappings
                if tab_name not in self.saved_mappings:
                    self.saved_mappings[tab_name] = {}
                self.saved_mappings[tab_name].update(mappings)
                self._save_mappings(self.saved_mappings)
                return mappings
            elif confirm == 'n':
                # Start over
                return self.review_mappings(
                    {k: None for k in mappings.keys()},
                    list(mappings.keys()),
                    tab_name,
                    preview_data
                )
            else:
                print("Please enter 'y' or 'n'")

    def get_mandatory_fields(self, tab_name: str) -> Set[str]:
        """Get truly mandatory fields that cannot be derived."""
        tab_schema = self.schema.get(tab_name, {})
        
        # Define fields that can be derived from other fields
        derivable_fields = {
            'Users': {
                'user_id',     # Can be derived from email
                'username',    # Can be derived from email
                'first_name',  # Can be derived from full_name
                'last_name'    # Can be derived from full_name
            },
            'Groups': {'group_id'},  # Can be derived from group_name
            'Roles': {'role_id'},    # Can be derived from role_name
            'Resources': {'resource_id'}  # Can be derived from resource_name
        }
        
        # Get all mandatory fields from schema
        mandatory_fields = {
            field for field, details in tab_schema.items()
            if details.get('mandatory', False)
        }
        
        # Remove fields that can be derived for this tab
        if tab_name in derivable_fields:
            mandatory_fields -= derivable_fields[tab_name]
        
        return mandatory_fields

    def start_over_mapping(self, input_headers: List[str], tab_name: str, preview_data: pd.DataFrame) -> Dict[str, str]:
        """Start the mapping process over from scratch."""
        clear_screen()
        
        print(f"\n{Fore.YELLOW}╔════════════════════ RESET INITIATED ═══════════════════╗")
        print("║ All mappings have been cleared                          ║")
        print("╚════════════════════════════════════════════════════════╝")
        
        # Clear all mappings for this tab
        if tab_name in self.saved_mappings:
            del self.saved_mappings[tab_name]
        
        # Initialize empty mappings
        mappings = {}
        
        # Get schema for this tab
        tab_schema = self.schema.get(tab_name, {})
        valid_fields = set(tab_schema.keys())
        
        # Get unmapped mandatory fields that cannot be derived
        mandatory_fields = self.get_mandatory_fields(tab_name)
        
        input("\nPress Enter to begin interactive mapping...")
        clear_screen()

        # Start interactive mapping process
        return self._interactive_mapping_flow(input_headers, tab_name, preview_data, valid_fields, mandatory_fields)

    def _interactive_mapping_flow(self, input_headers: List[str], tab_name: str, 
                                preview_data: pd.DataFrame, valid_fields: Set[str],
                                mandatory_fields: Set[str]) -> Dict[str, str]:
        """Interactive mapping flow for fields."""
        mappings = {}
        
        print(f"\n{Fore.CYAN}Starting mapping process for {tab_name}{Style.RESET_ALL}")
        
        # First map mandatory fields
        if mandatory_fields:
            print(f"\n{Fore.CYAN}Mapping mandatory fields:{Style.RESET_ALL}")
            for field in mandatory_fields:
                mapped = self._map_single_field(field, input_headers, preview_data, mappings, tab_schema)
                if not mapped:
                    print(f"{Fore.YELLOW}Note: '{field}' not mapped - will need to be derived{Style.RESET_ALL}")
        
        # Then map optional fields
        print(f"\n{Fore.CYAN}Mapping optional fields:{Style.RESET_ALL}")
        optional_fields = valid_fields - mandatory_fields
        for field in optional_fields:
            self._map_single_field(field, input_headers, preview_data, mappings, tab_schema)
        
        return mappings

    def _map_single_field(self, target_field: str, input_headers: List[str], 
                         preview_data: pd.DataFrame, mappings: Dict[str, str],
                         tab_schema: Dict) -> bool:
        """Handle mapping for a single field. Returns True if mapping was successful."""
        print(f"\nMapping field: {target_field}")
        
        # Get potential matches using fuzzy matching
        matches = process.extract(target_field, input_headers, limit=len(input_headers))
        potential_matches = [(header, score) for header, score in matches if score > 50]
        
        if potential_matches:
            print("\nPotential matches:")
            for idx, (header, score) in enumerate(potential_matches, 1):
                print(f"{idx}) {header} (match score: {score}%)")
                if header in preview_data.columns:
                    sample_values = preview_data[header].dropna().unique()[:3].tolist()
                    print(f"   Sample values: {sample_values}")
        else:
            print("No potential matches found")
        
        print("\nOptions:")
        print("1-N) Select a header")
        print("s) Skip this field")
        
        choice = input("Choose option (1-N or s): ").lower().strip()
        
        if choice == 's':
            return False
        
        try:
            idx = int(choice)
            if 1 <= idx <= len(potential_matches):
                header = potential_matches[idx-1][0]
                mappings[header] = target_field
                return True
        except ValueError:
            pass
        
        return False

    def print_mappings_preview(self, mappings: Dict[str, str], data: Dict[str, pd.DataFrame]) -> None:
        """Display current mappings with sample data."""
        print(f"\n{Fore.CYAN}Current Mappings{Style.RESET_ALL}")
        print("=" * 80)

        for source_header, target_field in mappings.items():
            # Find which sheet contains this header
            for sheet_name, df in data.items():
                if source_header in df.columns:
                    samples = df[source_header].dropna().unique()[:3].tolist()
                    samples_str = ", ".join(str(s) for s in samples)
                    print(f"{Fore.WHITE}'{target_field}' ← {Fore.GREEN}'{source_header}'{Style.RESET_ALL}")
                    print(f"   Samples: [{samples_str}]")
                    break

        print("\nOptions:")
        print("1-N) Select target field to modify")
        print("c) Continue with current mappings")
        print("s) Start over (clear all mappings)")
        print("\nChoose option (number, c, or s):")

    def detect_possible_tabs(self, headers: List[str]) -> List[str]:
        """Detect which tabs are likely present based on column headers."""
        detected_tabs = []
        headers_lower = [h.lower() for h in headers]
        
        # Define signature columns for each tab type
        tab_signatures = {
            'Users': ['user', 'username', 'email'],
            'Groups': ['group', 'groupname'],
            'Roles': ['role', 'permission', 'access'],
            'Resources': ['resource', 'application']
        }
        
        for tab_name, signatures in tab_signatures.items():
            if any(any(sig in h for h in headers_lower) for sig in signatures):
                detected_tabs.append(tab_name)
        
        return detected_tabs
