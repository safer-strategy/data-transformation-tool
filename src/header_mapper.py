import json
import os
from pathlib import Path
import logging
from fuzzywuzzy import process
from typing import List, Dict, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

class HeaderMapper:
    def __init__(self, schema_file: str):
        with open(schema_file, 'r') as f:
            self.schema = json.load(f)
        self.mappings_file = Path('mappings_history.json')
        self.load_saved_mappings()
        self.logger = logging.getLogger(__name__)

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

    def save_mappings(self, mappings):
        """Save mappings including explicitly skipped headers."""
        try:
            with open(self.mappings_file, 'w') as f:
                json.dump(self.saved_mappings, f, indent=2)
            self.logger.info("Mappings saved successfully")
        except Exception as e:
            self.logger.error(f"Error saving mappings: {e}")

    def map_headers(self, input_headers: List[str], tab_name: str) -> Dict[str, str]:
        """Map headers using saved mappings and fuzzy matching."""
        mappings = {}
        
        # Track used target fields to prevent duplicates
        used_targets = set()
        
        for header in input_headers:
            mapping_key = f"{tab_name}:{header}"
            
            # Check if this header was previously mapped or explicitly skipped
            if mapping_key in self.saved_mappings:
                saved_mapping = self.saved_mappings[mapping_key]
                if saved_mapping is None:  # Previously skipped
                    self.logger.debug(f"Skipping previously unmapped header: {header}")
                    mappings[header] = None
                    continue
                elif saved_mapping not in used_targets:  # Previously mapped and target not used
                    mappings[header] = saved_mapping
                    used_targets.add(saved_mapping)
                    continue
                else:  # Previously mapped but target already used
                    self.logger.warning(f"Duplicate target mapping found for '{header}', marking for review")
                    mappings[header] = None
                    continue

            # Only proceed with mapping if header wasn't previously handled
            tab_schema = self.schema.get(tab_name, {})
            
            # Try exact matches and synonyms
            mapped = False
            for field, details in tab_schema.items():
                if field not in used_targets and (
                    header.lower() in [s.lower() for s in details.get("synonyms", [])] or 
                    header.lower() == field.lower()
                ):
                    mappings[header] = field
                    used_targets.add(field)
                    mapped = True
                    break
            
            if not mapped:
                mappings[header] = None  # Will be handled in review_mappings if not previously skipped

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
        """Get fuzzy matches for a header with their scores."""
        choices = list(tab_schema.keys())
        
        # Add synonyms to choices for better matching
        expanded_choices = []
        for field, details in tab_schema.items():
            expanded_choices.append(field)
            if "synonyms" in details:
                for synonym in details["synonyms"]:
                    expanded_choices.append((synonym, field))  # Store original field name with synonym
        
        # Get matches including synonyms
        matches = process.extract(header.lower(), [c[0] if isinstance(c, tuple) else c for c in expanded_choices], limit=len(expanded_choices))
        
        # Convert matches back to original field names and remove duplicates while keeping highest scores
        seen_fields = {}
        for match, score in matches:
            field = next((c[1] for c in expanded_choices if isinstance(c, tuple) and c[0] == match), match)
            if field not in seen_fields or score > seen_fields[field][1]:
                seen_fields[field] = (field, score)
        
        # Sort by score and take top matches
        final_matches = sorted(seen_fields.values(), key=lambda x: x[1], reverse=True)[:num_matches]
        return final_matches

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
        """Review and confirm mappings with focus on mandatory fields first.

        Args:
            mappings: Dictionary of current header mappings
            input_headers: List of input headers to be mapped
            tab_name: Name of the current tab being processed
            preview_data: DataFrame containing sample data for preview

        Returns:
            Dict[str, str]: Updated mappings dictionary
        """
        used_targets: set = set(target for target in mappings.values() if target is not None)
        
        # Get schema rules for this tab
        tab_schema = self.schema.get(tab_name, {})
        
        # Separate mandatory and optional fields
        mandatory_fields = {
            field: details for field, details in tab_schema.items() 
            if details.get('mandatory', False)
        }
        optional_fields = {
            field: details for field, details in tab_schema.items() 
            if not details.get('mandatory', False)
        }
        
        # Show start over option
        self.print_separator('=')
        print(f"Tab: {tab_name}")
        print("\nRequired fields according to schema:")
        for field in mandatory_fields:
            mapped = any(target == field for target in mappings.values())
            status = "✓" if mapped else "✗"
            print(f"{status} {field}")
        
        print("\nCurrent mappings:")
        for header, target in mappings.items():
            if target:
                is_mandatory = target in mandatory_fields
                print(f"'{header}' → '{target}' {'(required)' if is_mandatory else ''}")
        
        print("\nOptions:")
        print("1) Continue with current mappings")
        print("2) Start over (clear all mappings)")
        
        while True:
            choice = input("Choose option (1-2): ").strip()
            if choice == "2":
                keys_to_remove = [k for k in self.saved_mappings.keys() if k.startswith(f"{tab_name}:")]
                for k in keys_to_remove:
                    self.saved_mappings.pop(k, None)
                mappings = {header: None for header in input_headers}
                used_targets = set()
                print("\nAll mappings cleared. Starting over...\n")
                break
            elif choice == "1":
                print("\nContinuing with current mappings...\n")
                break
            else:
                print("Please enter 1 or 2")

        # First pass: Handle mandatory fields
        print("\n=== MANDATORY FIELDS ===")
        unmapped_mandatory = set(mandatory_fields.keys()) - used_targets
        
        # Special handling for Groups tab
        if tab_name == "Groups":
            if "group_id" in unmapped_mandatory and any(
                mappings[h] == "group_name" for h in mappings
            ):
                print("\nNote: group_id will be auto-generated from group_name")
                unmapped_mandatory.remove("group_id")
                mappings["__generated_group_id__"] = "group_id"
                used_targets.add("group_id")
        
        if unmapped_mandatory:
            print(
                f"The following required fields need mapping: {', '.join(unmapped_mandatory)}"
            )
            
            for header in input_headers:
                if header in mappings and mappings[header] in mandatory_fields:
                    continue
                
                mapping_key = f"{tab_name}:{header}"
                
                # Show data preview
                preview = preview_data[header].head(3).tolist()
                matches = [
                    (field, self.calculate_match_score(header, field))
                    for field in unmapped_mandatory
                ]
                matches.sort(key=lambda x: x[1], reverse=True)
                
                if matches:
                    self.print_separator('-')
                    print(f"\nField: {header}")
                    print(f"Sample values: {preview}")
                    print("\nPossible mandatory field matches:")
                    
                    for idx, (match, score) in enumerate(matches, 1):
                        print(f"{idx}) {match} ({score}%) - REQUIRED")
                    
                    skip_option = (
                        "s) Skip this field"
                        if tab_name == "Groups" and "group_name" in used_targets
                        else ""
                    )
                    print(skip_option)
                    
                    while True:
                        skip_text = (
                            ", s to skip"
                            if tab_name == "Groups" and "group_name" in used_targets
                            else ""
                        )
                        choice = input(
                            f"Map '{header}' to which required field? "
                            f"(1-{len(matches)}{skip_text}): "
                        )
                        
                        if (
                            choice.lower() == 's'
                            and tab_name == "Groups"
                            and "group_name" in used_targets
                        ):
                            # Allow skipping group_id if group_name is present
                            if "group_id" in unmapped_mandatory:
                                unmapped_mandatory.remove("group_id")
                                mappings["__generated_group_id__"] = "group_id"
                                used_targets.add("group_id")
                                print("✓ group_id will be auto-generated from group_name")
                            break
                        
                        try:
                            idx = int(choice)
                            if 1 <= idx <= len(matches):
                                selected_field = matches[idx-1][0]
                                if selected_field not in used_targets:
                                    mappings[header] = selected_field
                                    self.saved_mappings[mapping_key] = selected_field
                                    used_targets.add(selected_field)
                                    unmapped_mandatory.remove(selected_field)
                                    print(
                                        f"✓ Mapped required field: '{header}' → "
                                        f"'{selected_field}'"
                                    )
                                    break
                                else:
                                    print(
                                        f"'{selected_field}' is already mapped to "
                                        "another column"
                                    )
                        except ValueError:
                            skip_text = (
                                " or s to skip"
                                if tab_name == "Groups" and "group_name" in used_targets
                                else ""
                            )
                            print(
                                f"Please enter a number between 1 and {len(matches)}"
                                f"{skip_text}"
                            )

        # Verify all mandatory fields are mapped
        remaining_mandatory = set(mandatory_fields.keys()) - used_targets
        if remaining_mandatory:
            print("\nWARNING: Not all mandatory fields are mapped!")
            print(f"Missing: {', '.join(remaining_mandatory)}")
            if not input("Continue anyway? (y/n): ").lower().startswith('y'):
                return self.review_mappings(mappings, input_headers, tab_name, preview_data)
        
        # Second pass: Handle optional fields
        print("\n=== OPTIONAL FIELDS ===")
        for header in input_headers:
            if mappings[header] is not None:
                continue
            
            mapping_key = f"{tab_name}:{header}"
            preview = preview_data[header].head(3).tolist()
            
            # Get matches from optional fields only
            matches = [(field, self.calculate_match_score(header, field))
                      for field in optional_fields if field not in used_targets]
            matches.sort(key=lambda x: x[1], reverse=True)
            
            if matches:
                self.print_separator('-')
                print(f"\nField: {header}")
                print(f"Sample values: {preview}")
                print("\nPossible optional field matches:")
                for idx, (match, score) in enumerate(matches, 1):
                    print(f"{idx}) {match} ({score}%)")
                print("s) Skip this field")
                
                while True:
                    choice = input(f"Map '{header}' to which optional field? (1-{len(matches)}, s to skip): ").lower()
                    if choice == 's':
                        mappings[header] = None
                        self.saved_mappings[mapping_key] = None
                        print(f"Skipped optional field: '{header}'")
                        break
                    try:
                        idx = int(choice)
                        if 1 <= idx <= len(matches):
                            selected_field = matches[idx-1][0]
                            if selected_field not in used_targets:
                                mappings[header] = selected_field
                                self.saved_mappings[mapping_key] = selected_field
                                used_targets.add(selected_field)
                                print(f"✓ Mapped optional field: '{header}' → '{selected_field}'")
                                break
                            else:
                                print(f"'{selected_field}' is already mapped to another column")
                    except ValueError:
                        print(f"Please enter a number between 1 and {len(matches)} or 's' to skip")
    
        # Save and return
        self.save_mappings(self.saved_mappings)
        return {k: v for k, v in mappings.items() if v is not None}

    def calculate_match_score(self, source: str, target: str) -> int:
        """Calculate match score between source and target strings."""
        from difflib import SequenceMatcher
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
