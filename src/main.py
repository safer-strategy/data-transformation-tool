import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import pandas as pd

from reader import Reader
from header_mapper import HeaderMapper
from data_transformer import DataTransformer
from validator import Validator
from output_generator import OutputGenerator


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
    Read Excel sheets with user selection if multiple sheets exist.
    
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
        
        print("\nAvailable sheets:")
        for idx, name in enumerate(sheet_names, 1):
            print(f"{idx}) {name}")
        
        selected_sheets = []
        while True:
            choice = input(
                "\nSelect sheets to process (comma-separated numbers, or 'all'): "
            ).strip()
            
            if choice.lower() == 'all':
                selected_sheets = sheet_names
                break
            
            try:
                indices = [int(x.strip()) for x in choice.split(',')]
                selected_sheets = [
                    sheet_names[i-1] for i in indices 
                    if 1 <= i <= len(sheet_names)
                ]
                if selected_sheets:
                    break
                print("No valid sheets selected. Please try again.")
            except (ValueError, IndexError):
                print("Invalid selection. Please try again.")
        
        return {
            name: pd.read_excel(file_path, sheet_name=name) 
            for name in selected_sheets
        }
    
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        return None


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


def flatten_dataframes(
    data: Dict[str, pd.DataFrame],
    key_threshold: float = 0.8
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Flatten multiple dataframes using automatically detected key columns.
    
    Args:
        data: Dictionary of sheet names and their corresponding dataframes
        key_threshold: Minimum uniqueness ratio to consider a column as key
        
    Returns:
        Tuple containing:
        - Flattened dataframe or None if flattening fails
        - List of detected key columns
    """
    if not data:
        logging.error("No data to flatten")
        return None, []

    # Find potential key columns across all sheets
    key_columns = []
    for sheet_name, df in data.items():
        for column in df.columns:
            # Check if column has mostly unique values
            if df[column].nunique() / len(df) >= key_threshold:
                unique_ratio = df[column].nunique() / len(df)
                logging.info(
                    f"Potential key column found in {sheet_name}: "
                    f"{column} (uniqueness: {unique_ratio:.2f})"
                )
                if column not in key_columns:
                    key_columns.append(column)

    if not key_columns:
        logging.error("No suitable key columns found for flattening")
        return None, []

    # Start with the first dataframe as base
    base_sheet_name = list(data.keys())[0]
    result_df = data[base_sheet_name].copy()
    
    # Merge remaining dataframes
    for sheet_name, df in list(data.items())[1:]:
        common_keys = [col for col in key_columns if col in df.columns 
                      and col in result_df.columns]
        
        if not common_keys:
            logging.warning(
                f"No common key columns found for sheet: {sheet_name}, skipping"
            )
            continue

        try:
            # Merge using all common key columns
            result_df = pd.merge(
                result_df,
                df,
                on=common_keys,
                how='outer',
                suffixes=('', f'_{sheet_name}')
            )
            
            logging.info(
                f"Merged sheet {sheet_name} using keys: {common_keys}"
            )
            
            # Log missing data statistics
            missing_count = result_df.isna().sum()
            if missing_count.any():
                logging.warning(
                    f"Missing data after merging {sheet_name}:\n"
                    f"{missing_count[missing_count > 0]}"
                )
                
        except Exception as e:
            logging.error(f"Error merging sheet {sheet_name}: {str(e)}")
            return None, key_columns

    return result_df, key_columns


def main() -> int:
    """
    Main function with enhanced input processing and schema flattening.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    setup_logging()
    
    if len(sys.argv) != 2:
        logging.error("Usage: python src/main.py <input_file>")
        return 1
    
    input_path = sys.argv[1]
    if not validate_input_file(input_path):
        return 1
    
    current_dir = Path(__file__).parent
    schema_path = current_dir / 'schema.json'
    if not schema_path.exists():
        logging.error(f"Schema file not found at: {schema_path}")
        return 1
    
    try:
        print_header("Starting Data Processing")
        
        # Initialize components
        reader = Reader()
        mapper = HeaderMapper(str(schema_path))
        transformer = DataTransformer(str(schema_path))
        validator = Validator(str(schema_path))
        
        # Read input based on file type
        input_file = Path(input_path)
        if input_file.suffix.lower() == '.xlsx':
            data = read_excel_sheets(input_path)
        else:  # .csv
            data = {"Sheet1": pd.read_csv(input_path)}
        
        if not data:
            return 1
            
        # Flatten data if multiple sheets exist
        if len(data) > 1:
            print_header("Flattening Multiple Sheets")
            flattened_data, key_columns = flatten_dataframes(data)
            
            if flattened_data is not None:
                data = {"Flattened": flattened_data}
                logging.info(
                    f"Successfully flattened data using keys: {key_columns}"
                )
            else:
                logging.error("Failed to flatten data, processing sheets separately")
        
        # Load schema for output generator
        with open(schema_path) as f:
            schema = json.load(f)
        
        generator = OutputGenerator(schema)
        output_path = get_output_path(input_path)
        
        # Process data through the pipeline
        processed_data = reader.process_data(data)
        if not processed_data:
            return 1
            
        valid_data, invalid_data = validator.validate_data(processed_data)
        
        # Generate output files
        generator.generate_excel(valid_data, output_path)
        if invalid_data:
            invalid_path = output_path.replace('.xlsx', '_invalid.xlsx')
            generator.generate_excel(invalid_data, invalid_path)
        
        print("\n✓ Processing completed successfully!")
        return 0

    except Exception as e:
        logging.error(f"Error during processing: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())