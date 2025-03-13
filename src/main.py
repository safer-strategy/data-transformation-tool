import os
import sys
import logging
import json
from pathlib import Path
from reader import Reader
from header_mapper import HeaderMapper
from data_transformer import DataTransformer
from validator import Validator
from output_generator import OutputGenerator

# Configure logging to output to both file and console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('validation.log')
    ]
)

# Force stdout to flush immediately
sys.stdout.reconfigure(line_buffering=True)

logger = logging.getLogger(__name__)

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
        logger.error("Usage: python main.py <input_file_or_directory>")
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)

    # Get output path
    output_path = get_output_path(input_path)
    
    # Get the absolute path to schema.json
    current_dir = Path(__file__).parent
    schema_path = current_dir / 'schema.json'
    
    if not schema_path.exists():
        logger.error(f"Schema file not found at: {schema_path}")
        sys.exit(1)

    try:
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
        logger.info(f"Reading input from: {input_path}")
        data = reader.read_files(input_path)
        
        # Map headers
        mappings = {}
        for tab_name, df in data.items():
            logger.info(f"Processing tab: {tab_name}")
            mappings[tab_name] = mapper.map_headers(df.columns, tab_name)
            mappings[tab_name] = mapper.review_mappings(
                mappings[tab_name],
                df.columns,
                tab_name,
                df
            )
        
        # Transform data
        logger.info("Transforming data...")
        transformed_data = transformer.transform_data(data, mappings)
        
        # Split and process relationships
        entity_data = {
            k: v for k, v in transformed_data.items() 
            if k in ["Users", "Groups", "Roles", "Resources"]
        }
        rel_data = {
            k: v for k, v in transformed_data.items() 
            if k not in entity_data
        }
        
        # Resolve relationships
        logger.info("Resolving relationships...")
        transformed_rel = transformer.resolve_relationships(entity_data, rel_data)
        transformed_data.update(transformed_rel)
        
        # Validate and separate valid/invalid records
        logger.info("Validating data...")
        valid_data, invalid_data = validator.validate_data(transformed_data)
        
        # Generate output for valid records
        logger.info(f"Generating output file: {output_path}")
        generator.generate_excel(valid_data, output_path)
        
        # Generate output for invalid records if any exist
        if any(not df.empty for df in invalid_data.values()):
            invalid_path = str(Path(output_path).parent / f"invalid_records_{Path(output_path).name}")
            logger.info(f"Generating invalid records file: {invalid_path}")
            generator.generate_excel(invalid_data, invalid_path)
        
        logger.info("Processing completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())