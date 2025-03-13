import os
import sys
import json
from pathlib import Path
from reader import Reader
from header_mapper import HeaderMapper
from data_transformer import DataTransformer
from validator import Validator
from output_generator import OutputGenerator
import logging

def setup_logging(debug_mode=False):
    """Configure logging settings."""
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(message)s',  # Simplified format for better UX
        handlers=[
            logging.FileHandler('validation.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Suppress debug messages from other modules
    if not debug_mode:
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('PIL').setLevel(logging.WARNING)

def print_header(text):
    """Print a formatted header."""
    print(f"\n{'='*80}\n{text.center(80)}\n{'='*80}\n")

def get_output_path(input_path: str) -> str:
    """Generate output path based on input path."""
    input_path = Path(input_path)
    converts_dir = Path("converts")
    converts_dir.mkdir(exist_ok=True)
    
    # Generate output filename
    output_name = f"converted_{input_path.stem}.xlsx"
    return str(converts_dir / output_name)

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <input_file_or_directory>")
        sys.exit(1)

    # Set up logging without debug messages
    setup_logging(debug_mode=False)
    logger = logging.getLogger(__name__)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)

    # Get output path
    output_path = get_output_path(input_path)
    
    # Get schema path
    current_dir = Path(__file__).parent
    schema_path = current_dir / 'schema.json'
    
    if not schema_path.exists():
        logger.error(f"Schema file not found at: {schema_path}")
        sys.exit(1)

    try:
        print_header("Starting Data Processing")
        
        # Initialize components
        reader = Reader()
        mapper = HeaderMapper(str(schema_path))
        transformer = DataTransformer(str(schema_path))
        validator = Validator(str(schema_path))
        
        # Load schema for output generator
        with open(schema_path) as f:
            schema = json.load(f)
        generator = OutputGenerator(schema)

        # Read input
        print("\nReading input files...")
        data = reader.read_files(input_path)
        
        # Map headers
        mappings = {}
        for tab_name, df in data.items():
            print_header(f"Processing {tab_name} Tab")
            
            # Check if tab is empty
            if df.empty:
                print(f"\nSkipping empty tab: {tab_name}")
                continue

            # Ask if user wants to process this tab
            while True:
                choice = input(f"\nProcess '{tab_name}' tab? (y/n): ").lower()
                if choice in ['y', 'n']:
                    break
                print("Please enter 'y' for yes or 'n' for no.")

            if choice == 'n':
                print(f"\nSkipping tab: {tab_name}")
                continue
            
            # First attempt automatic mapping based on synonyms
            initial_mappings = mapper.map_headers(df.columns, tab_name)
            
            # Check for missing mandatory fields
            mandatory_fields = mapper.get_mandatory_fields(tab_name)
            mapped_mandatory = set(v for v in initial_mappings.values() if v in mandatory_fields)
            missing_mandatory = set(mandatory_fields) - mapped_mandatory
            
            if missing_mandatory:
                print("\nMandatory fields requiring mapping:")
                for field in missing_mandatory:
                    print(f"  • {field}")
            
            # Review and confirm mappings
            mappings[tab_name] = mapper.review_mappings(
                initial_mappings,
                df.columns,
                tab_name,
                df
            )
            
            print(f"\n✓ Completed mapping for {tab_name}")
        
        # Transform data
        print("\nTransforming data...")
        transformed_data = {}
        for tab_name, df in data.items():
            if tab_name not in mappings:
                continue
                
            try:
                print(f"\nTransforming {tab_name} tab...")
                transformed_df = transformer.transform_data_tab(
                    df, 
                    mappings[tab_name],
                    tab_name
                )
                if not transformed_df.empty:
                    transformed_data[tab_name] = transformed_df
            except Exception as e:
                logger.error(f"Error transforming {tab_name} tab: {str(e)}")
                print(f"⚠️  Failed to transform {tab_name} tab. Skipping...")
                continue
        
        if not transformed_data:
            raise Exception("No data was successfully transformed")
        
        # Process relationships
        print("\nProcessing relationships...")
        entity_data = {k: v for k, v in transformed_data.items() 
                      if k in ["Users", "Groups", "Roles", "Resources"]}
        rel_data = {k: v for k, v in transformed_data.items() 
                   if k not in entity_data}
        
        transformed_rel = transformer.resolve_relationships(entity_data, rel_data)
        transformed_data.update(transformed_rel)
        
        # Validate data
        print("\nValidating data...")
        valid_data, invalid_data = validator.validate_data(transformed_data)
        
        # Generate output
        print(f"\nGenerating output file: {output_path}")
        generator.generate_excel(valid_data, output_path)
        
        if any(not df.empty for df in invalid_data.values()):
            invalid_path = str(Path(output_path).parent / f"invalid_records_{Path(output_path).name}")
            print(f"Generating invalid records file: {invalid_path}")
            generator.generate_excel(invalid_data, invalid_path)
        
        print("\n✓ Processing completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())