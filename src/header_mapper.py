import json
import os
from pathlib import Path
import logging
from fuzzywuzzy import process
from typing import List, Dict, Tuple, Set
import pandas as pd
from difflib import SequenceMatcher
import yaml

logger = logging.getLogger(__name__)

class HeaderMapper:
    def __init__(self, schema_file: str):
        self.schema_file = schema_file
        self.logger = logging.getLogger(__name__)
        
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

    def map_headers(self, input_headers: List[str], tab_name: str) -> Dict[str, str]:
        """Automatically map headers based on schema only."""
        mappings = {}
        
        # Get schema for this tab
        tab_schema = self.schema.get(tab_name, {})
        valid_fields = set(tab_schema.keys())  # Only fields defined in schema
        
        # Only attempt to map headers that could potentially map to valid fields
        for header in input_headers:
            # First try direct mappings from schema synonyms
            for field, props in tab_schema.items():
                if header.lower() in [syn.lower() for syn in props.get('synonyms', [])]:
                    mappings[header] = field
                    break

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
        # Filter out any mappings to fields not in schema
        tab_schema = self.schema.get(tab_name, {})
        valid_fields = set(tab_schema.keys())
        mappings = {k: v for k, v in mappings.items() if v in valid_fields}

        # First check if we have saved mappings for this tab
        for header in input_headers:
            mapping_key = f"{tab_name}:{header}"
            if mapping_key in self.saved_mappings:
                mappings[header] = self.saved_mappings[mapping_key]

        print("\nCurrent mappings:")
        for header, target in mappings.items():
            if target:
                print(f"'{header}' → '{target}'")

        # Get mandatory fields from schema
        mandatory_fields = self.get_mandatory_fields(tab_name)

        # Check for unmapped mandatory fields
        mapped_fields = set(mappings.values())
        unmapped_mandatory = mandatory_fields - mapped_fields
        if unmapped_mandatory:
            print("\nMandatory fields requiring mapping:")
            for field in unmapped_mandatory:
                print(f"  • {field}")

        print("\nOptions:")
        print("1) Continue with current mappings")
        print("2) Start over (clear all mappings)")
        choice = input("Choose option (1-2): ")
        
        if choice == "2":
            return self.start_over_mapping(input_headers, tab_name, preview_data)

        # Save confirmed mappings
        for header, target in mappings.items():
            if target:
                mapping_key = f"{tab_name}:{header}"
                self.saved_mappings[mapping_key] = target

        self.save_mappings(mappings)
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
        
        # Special cases where fields can be derived
        derivable_fields = {
            'Groups': {'group_id'},  # group_id can be derived from group_name
            'Roles': {'role_id'},    # role_id can be derived from role_name
            'Resources': {'resource_id'}  # resource_id can be derived from resource_name
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
        print("\nStarting mapping process over...")
        
        mappings = {}
        tab_schema = self.schema.get(tab_name, {})
        valid_fields = set(tab_schema.keys())
        
        # Get unmapped mandatory fields first
        mandatory_fields = self.get_mandatory_fields(tab_name)
        mapped_fields = set()

        # First, handle mandatory fields
        print("\nMapping mandatory fields:")
        for mandatory_field in mandatory_fields:
            if mandatory_field not in mapped_fields:
                self._map_single_field(mandatory_field, input_headers, preview_data, mappings, tab_schema)
                if mandatory_field in [v for v in mappings.values()]:
                    mapped_fields.add(mandatory_field)

        # Then handle optional fields that are in the schema
        print("\nMapping optional fields:")
        optional_fields = valid_fields - mandatory_fields
        for field in optional_fields:
            if field not in mapped_fields:
                self._map_single_field(field, input_headers, preview_data, mappings, tab_schema)

        return mappings

    def _map_single_field(self, target_field: str, input_headers: List[str], 
                         preview_data: pd.DataFrame, mappings: Dict[str, str],
                         tab_schema: Dict) -> None:
        """Handle mapping for a single field."""
        # Skip if field is already mapped
        if target_field in mappings.values():
            return

        print(f"\nMapping for field: '{target_field}'")
        
        # Get potential matches based on schema synonyms
        potential_matches = []
        field_props = tab_schema[target_field]
        field_synonyms = field_props.get('synonyms', [])
        
        for header in input_headers:
            if header in mappings:  # Skip already mapped headers
                continue
            
            score = max(
                self.calculate_match_score(header.lower(), syn.lower())
                for syn in field_synonyms + [target_field]
            )
            if score >= 60:
                potential_matches.append((header, score))
        
        potential_matches.sort(key=lambda x: x[1], reverse=True)
        
        if potential_matches:
            print("\nPossible matches:")
            for idx, (header, score) in enumerate(potential_matches, 1):
                print(f"{idx}) {header} ({score}%)")
                if header in preview_data:
                    sample_values = preview_data[header].dropna().unique()[:3].tolist()
                    print(f"   Sample values: {sample_values}")
        
            print("\nOptions:")
            print("1-N) Select a header")
            print("s) Skip this field")
            
            choice = input("Choose option (1-N or s): ")
            if choice.lower() != 's':
                try:
                    idx = int(choice)
                    if 1 <= idx <= len(potential_matches):
                        header = potential_matches[idx-1][0]
                        mappings[header] = target_field
                except ValueError:
                    pass
