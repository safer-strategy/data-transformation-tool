import os
import sys
import time
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import pandas as pd
from colorama import Fore, Style, init
import logging
from tqdm import tqdm
import fnmatch
from header_mapper import HeaderMapper
from data_transformer import DataTransformer
from validator import Validator  # Change this import to use the correct class
from reader import Reader

# Initialize colorama
init(strip=False)  # Allow color stripping based on --no-color

def print_banner():
    print("╔════════════════════════════════════════╗")
    print("║    AMT-8000 Power Up Successful       ║")
    print("╚════════════════════════════════════════╝")
    print("\n")

class AMT8000CLI:
    def __init__(self, no_color: bool = False, page_size: int = 5):
        self.no_color = no_color
        if self.no_color:
            init(strip=True)  # Strip all ANSI codes if --no-color is set
        
        self.page_size = page_size
        self.mission_statement = "CONSTRUCT APPMAP FROM INPUT SIGNALS"
        
        # Set up paths
        self.base_dir = Path.cwd()
        self.transmission_dir = self.base_dir / "converts"
        self.transmission_log = self.transmission_dir / "transmission.log"
        self.schema_file = self.base_dir / "src" / "schema.json"  # Add schema file path
        
        # Create transmission directory if it doesn't exist
        self.transmission_dir.mkdir(exist_ok=True)
        
        # Initialize transmission log
        if not self.transmission_log.exists():
            with open(self.transmission_log, 'w') as f:
                f.write("AMT-8000 TRANSMISSION LOG\n")
                f.write("=" * 50 + "\n")

    def print_styled(self, text: str, color: str = "") -> None:
        """Print text with optional color styling."""
        if self.no_color:
            print(text)
        else:
            print(f"{color}{text}{Style.RESET_ALL}")

    def boot_sequence(self) -> None:
        """Execute the retro sci-fi boot sequence."""
        self.print_styled("\n> INITIALIZING AMT-8000 SYSTEMS", Fore.CYAN)
        time.sleep(0.5)
        
        # System checks
        checks = [
            ("POWER SYSTEMS", "NOMINAL"),
            ("QUANTUM BUFFER", "ALIGNED"),
            ("NEURAL NETWORK", "CALIBRATED"),
            ("DATA MATRICES", "INITIALIZED")
        ]
        
        for system, status in checks:
            self.print_styled(f"> CHECKING {system}...", Fore.YELLOW)
            time.sleep(0.3)
            self.print_styled(f"  STATUS: {status}", Fore.GREEN)
        
        # Mission statement
        self.print_styled("\n> MISSION PARAMETERS LOADED", Fore.CYAN)
        self.print_styled(f"  {self.mission_statement}", Fore.GREEN)
        time.sleep(0.5)

    def scan_phase(self, directory: str) -> List[Path]:
        """Scan directory for input files and display sample data."""
        self.print_styled("\n> INITIATING DIRECTORY SCAN", Fore.CYAN)
        
        path = Path(directory).resolve()
        if not path.exists():
            self.print_styled(f"> ERROR: Directory {directory} not found", Fore.RED)
            return []
            
        files = []
        for ext in ['.xlsx', '.csv']:
            files.extend(list(path.glob(f'*{ext}')))
        
        if not files:
            self.print_styled(f"> NO VALID INPUT SIGNALS DETECTED IN: {path}", Fore.RED)
            self.print_styled("> PLEASE PLACE .XLSX OR .CSV FILES IN THE UPLOADS DIRECTORY", Fore.YELLOW)
            return []
        
        self.print_styled(f"\n> DETECTED {len(files)} POTENTIAL INPUT SIGNAL(S)", Fore.GREEN)
        
        # Display files with sample data
        for idx, file in enumerate(files, start=1):
            self.print_styled(f"\n> SIGNAL {idx}/{len(files)}: {file.name}", Fore.CYAN)
            
            try:
                # Read sample data
                df = pd.read_excel(file) if file.suffix == '.xlsx' else pd.read_csv(file)
                
                # Display sample info
                self.print_styled("  SIGNAL PROPERTIES:", Fore.YELLOW)
                print(f"  - Dimensions: {df.shape[0]} x {df.shape[1]}")
                print("  - Headers: " + ", ".join(df.columns[:5]) + 
                      ("..." if len(df.columns) > 5 else ""))
                
            except Exception as e:
                self.print_styled(f"  ERROR: {str(e)}", Fore.RED)
        
        # Only show pagination if we have more than page_size files
        if len(files) > self.page_size:
            self.print_styled("\n> PRESS ENTER TO CONTINUE", Fore.YELLOW)
            input()
        
        return files

    def alignment_phase(self, files: List[Path]) -> Dict[str, Dict[str, str]]:
        """Execute the alignment phase for mapping columns."""
        self.print_styled("\n> INITIATING ALIGNMENT PHASE", Fore.CYAN)
        
        all_mappings = {}
        for file in files:
            file_mappings = {}
            
            # Read the file first
            df = pd.read_excel(file) if file.suffix == '.xlsx' else pd.read_csv(file)
            
            # Detect which tabs we can map based on the columns
            mapper = HeaderMapper(str(self.schema_file))
            detected_tabs = mapper.detect_possible_tabs(list(df.columns))
            
            if not detected_tabs:
                self.print_styled(f"\n! NO RECOGNIZABLE DATA STRUCTURE IN: {file.name}", Fore.RED)
                continue
                
            self.print_styled(f"\n> DETECTED TAB TYPES: {', '.join(detected_tabs)}", Fore.CYAN)
            
            # Process only detected tabs
            for tab_name in detected_tabs:
                self.print_styled(f"\n> ANALYZING SIGNAL FOR {tab_name}: {file.name}", Fore.YELLOW)
                
                try:
                    self.print_styled(f"\n> PROCESSING {tab_name} DATA", Fore.CYAN)
                    
                    # Get initial mappings
                    mappings = mapper.map_headers(list(df.columns), tab_name)
                    
                    # Review and modify mappings
                    self.print_styled(f"\n> CURRENT FIELD ALIGNMENTS FOR {tab_name}", Fore.CYAN)
                    print("=" * 60)
                    
                    while True:
                        # Display current mappings with retro style
                        for idx, (source, target) in enumerate(mappings.items(), 1):
                            sample_values = df[source].dropna().unique()[:3].tolist()
                            samples_str = ", ".join(str(s) for s in sample_values)
                            self.print_styled(f"{idx}. {source} → {target}", Fore.GREEN)
                            print(f"   Sample values: [{samples_str}]")
                        
                        self.print_styled("\n> ALIGNMENT OPTIONS", Fore.CYAN)
                        print("1-N) Select field number to modify mapping")
                        print("v) Validate mappings")
                        print("c) Continue with current mappings")
                        print("r) Reset all mappings")
                        print("s) Skip this tab")
                        
                        choice = input("\n> ENTER COMMAND: ").lower()
                        
                        if choice == 'c':
                            file_mappings[tab_name] = {
                                'mappings': mappings,
                                'tab_name': tab_name
                            }
                            break
                        elif choice == 's':
                            break
                        elif choice == 'v':
                            mandatory_fields = mapper.get_mandatory_fields(tab_name)
                            unmapped = [f for f in mandatory_fields if f not in mappings.values()]
                            if unmapped:
                                self.print_styled("\n! WARNING: MANDATORY FIELDS UNMAPPED", Fore.RED)
                                for field in unmapped:
                                    print(f"  - {field}")
                                input("\nPress Enter to continue...")
                            else:
                                self.print_styled("\n✓ ALL MANDATORY FIELDS MAPPED", Fore.GREEN)
                                input("\nPress Enter to continue...")
                        elif choice == 'r':
                            mappings = mapper.map_headers(list(df.columns), tab_name)
                        else:
                            try:
                                idx = int(choice)
                                if 1 <= idx <= len(mappings):
                                    source = list(mappings.keys())[idx-1]
                                    self.print_styled(f"\n> MODIFYING MAPPING FOR: {source}", Fore.CYAN)
                                    print("\nAvailable target fields:")
                                    
                                    # Get valid fields from schema for the selected tab
                                    valid_fields = sorted(mapper.schema.get(tab_name, {}).keys())
                                    for i, field in enumerate(valid_fields, 1):
                                        print(f"{i}. {field}")
                                    
                                    field_choice = input("\n> SELECT TARGET FIELD (1-N): ")
                                    try:
                                        field_idx = int(field_choice)
                                        if 1 <= field_idx <= len(valid_fields):
                                            mappings[source] = valid_fields[field_idx-1]
                                            self.print_styled(f"\n✓ MAPPING UPDATED: {source} → {mappings[source]}", Fore.GREEN)
                                    except ValueError:
                                        self.print_styled("\n! INVALID SELECTION", Fore.RED)
                            except ValueError:
                                self.print_styled("\n! INVALID COMMAND", Fore.RED)
                
                except Exception as e:
                    self.print_styled(f"  ERROR: {str(e)}", Fore.RED)
                    continue
            
            # Store all mappings for this file
            if file_mappings:
                all_mappings[str(file)] = file_mappings
        
        return all_mappings

    def view_transmission_history(self) -> None:
        """View transmission history with pagination."""
        self.print_styled("\n> ACCESSING TRANSMISSION LOGS", Fore.CYAN)
        
        try:
            with open(self.transmission_log, 'r') as f:
                lines = f.readlines()[2:]  # Skip header lines
            
            if not lines:
                self.print_styled("  NO TRANSMISSION HISTORY FOUND", Fore.YELLOW)
                return
            
            # Paginate through history
            total_pages = (len(lines) + self.page_size - 1) // self.page_size
            current_page = 1
            
            while True:
                clear_screen()
                self.print_styled(f"\n> TRANSMISSION LOG (Page {current_page}/{total_pages})", Fore.CYAN)
                print("=" * 50)
                
                start_idx = (current_page - 1) * self.page_size
                end_idx = start_idx + self.page_size
                
                for line in lines[start_idx:end_idx]:
                    self.print_styled(f"  {line.strip()}", Fore.WHITE)
                
                if total_pages > 1:
                    print("\nNavigation:")
                    print("n: Next page")
                    print("p: Previous page")
                    print("q: Quit to main menu")
                    
                    choice = input("\nEnter choice: ").lower()
                    if choice == 'n' and current_page < total_pages:
                        current_page += 1
                    elif choice == 'p' and current_page > 1:
                        current_page -= 1
                    elif choice == 'q':
                        break
                else:
                    input("\nPress Enter to continue...")
                    break
                    
        except Exception as e:
            self.print_styled(f"  ERROR ACCESSING LOG: {str(e)}", Fore.RED)

    def delete_transmissions(self, pattern: str) -> None:
        """Delete transmission files matching a pattern."""
        self.print_styled(f"\n> SEARCHING FOR TRANSMISSIONS MATCHING: {pattern}", Fore.CYAN)
        
        try:
            matching_files = list(self.transmission_dir.glob(f"*{pattern}*"))
            
            if not matching_files:
                self.print_styled("  NO MATCHING TRANSMISSIONS FOUND", Fore.YELLOW)
                return
            
            self.print_styled("\n  MATCHING TRANSMISSIONS:", Fore.YELLOW)
            for idx, file in enumerate(matching_files, 1):
                print(f"  {idx}. {file.name}")
            
            confirm = input("\nConfirm deletion (y/n): ").lower()
            if confirm == 'y':
                with tqdm(matching_files, desc="Deleting files") as pbar:
                    for file in pbar:
                        file.unlink()
                self.print_styled("  ✓ DELETION COMPLETE", Fore.GREEN)
            else:
                self.print_styled("  DELETION CANCELLED", Fore.YELLOW)
                
        except Exception as e:
            self.print_styled(f"  ERROR DURING DELETION: {str(e)}", Fore.RED)

    def transmission_phase(self, files: List[Path], mappings: Dict[str, Dict]) -> None:
        """Execute the transmission phase with progress bars."""
        self.print_styled("\n> INITIATING TRANSMISSION PHASE", Fore.CYAN)
        
        with tqdm(files, desc="Processing files") as pbar:
            for file in pbar:
                self.print_styled(f"\n> PROCESSING SIGNAL: {file.name}", Fore.YELLOW)
                
                try:
                    # Get mappings for this file
                    file_mappings = mappings.get(str(file))
                    if not file_mappings:
                        raise ValueError("No mapping configuration found")
                    
                    # Initialize transformer with schema
                    transformer = DataTransformer(str(self.schema_file))
                    
                    # Read and transform data for all tabs
                    all_data = {}
                    df = pd.read_excel(file) if file.suffix == '.xlsx' else pd.read_csv(file)
                    
                    for tab_name, tab_data in file_mappings.items():
                        self.print_styled(f"  Processing {tab_name}...", Fore.CYAN)
                        transformed_data = {}
                        for target_field, source_field in tab_data['mappings'].items():
                            if source_field in df.columns:
                                transformed_data[target_field] = df[source_field]
                        transformed_df = pd.DataFrame(transformed_data)
                        all_data[tab_name] = transformed_df
                    
                    # Transform all data according to schema rules
                    transformed_data = transformer.transform_data(all_data, file_mappings)
                    
                    # Initialize validator with schema
                    validator = Validator(transformer.schema)  # Change this to use Validator instead of DataValidator
                    valid_data, invalid_data = validator.validate_data(transformed_data)
                    
                    if invalid_data:
                        self.print_styled("\n! WARNING: INVALID RECORDS DETECTED", Fore.YELLOW)
                        for tab, invalid_df in invalid_data.items():
                            print(f"  - {tab}: {len(invalid_df)} invalid records")
                    
                    # Generate single output file with all tabs
                    output_file = self.transmission_dir / f"converted_{file.stem}.xlsx"
                    with pd.ExcelWriter(output_file) as writer:
                        for tab_name, df in valid_data.items():
                            df.to_excel(writer, sheet_name=tab_name, index=False)
                    
                    # Log the transmission
                    self.log_transmission(file, output_file)
                    
                    self.print_styled(f"\n✓ TRANSMISSION COMPLETE: {output_file.name}", Fore.GREEN)
                    
                except Exception as e:
                    self.print_styled(f"  ERROR: {str(e)}", Fore.RED)
                    continue

    def log_transmission(self, input_file: Path, output_file: Path) -> None:
        """Log transmission details."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {input_file.name} → {output_file.name}\n"
        
        with open(self.transmission_log, 'a') as f:
            f.write(log_entry)

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='AMT-8000 Data Transformation Tool')
    parser.add_argument('input_path', nargs='?', help='Input file or directory path')
    parser.add_argument('--no-color', action='store_true', help='Disable color output')
    parser.add_argument('--page-size', type=int, default=5, help='Number of items per page')
    parser.add_argument('--history', action='store_true', help='View transmission history')
    parser.add_argument('--delete', help='Delete transmissions matching pattern')
    args = parser.parse_args()

    # Print banner
    print_banner()

    # Initialize CLI
    cli = AMT8000CLI(no_color=args.no_color, page_size=args.page_size)

    try:
        # Execute boot sequence
        cli.boot_sequence()

        if args.history:
            cli.view_transmission_history()
        elif args.delete:
            cli.delete_transmissions(args.delete)
        elif args.input_path:
            # Execute main processing flow
            files = cli.scan_phase(args.input_path)
            if not files:
                sys.exit(1)

            mappings = cli.alignment_phase(files)
            if not mappings:
                sys.exit(1)

            cli.transmission_phase(files, mappings)
        else:
            parser.print_help()

    except KeyboardInterrupt:
        cli.print_styled("\n\n> OPERATION CANCELLED BY USER", Fore.YELLOW)
        sys.exit(1)
    except Exception as e:
        cli.print_styled(f"\n> CRITICAL ERROR: {str(e)}", Fore.RED)
        sys.exit(1)

if __name__ == "__main__":
    main()
