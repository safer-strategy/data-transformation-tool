import pandas as pd
from pathlib import Path
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class OutputGenerator:
    def __init__(self, schema: Dict):
        self.schema = schema
        # Define the correct tab order based on schema rules
        self.tab_order = [
            "Users",
            "Groups", 
            "Roles",
            "Resources",
            "User Groups",
            "User Roles",
            "Group Roles",
            "User Resources",
            "Role Resources",
            "Group Groups"
        ]
        # Define column orders for each tab
        self.column_orders = {
            "Groups": ["group_id", "group_name", "group_description"],
            # Add other tab column orders as needed
        }

    def generate_excel(self, data: Dict[str, pd.DataFrame], output_path: str) -> None:
        """Generate Excel output files."""
        try:
            # Log input data state
            logger.info("Input data summary:")
            for tab_name, df in data.items():
                logger.info(f"{tab_name}: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Filter columns and order sheets
            processed_data = self._process_data(data)
            
            # Log processed data state
            logger.info("Processed data summary:")
            for tab_name, df in processed_data.items():
                logger.info(f"{tab_name}: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Check if we have any non-empty DataFrames
            non_empty_data = {k: v for k, v in processed_data.items() if not v.empty}
            
            if not non_empty_data:
                logger.warning("No non-empty DataFrames found to write")
                # If all DataFrames are empty, create a dummy sheet with a message
                dummy_df = pd.DataFrame({'Message': ['No valid data found to export']})
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    dummy_df.to_excel(writer, sheet_name='Info', index=False)
            else:
                # Log what we're about to write
                logger.info("Writing non-empty data:")
                for tab_name, df in non_empty_data.items():
                    logger.info(f"{tab_name}: {df.shape[0]} rows, {df.shape[1]} columns")
                # Write non-empty DataFrames to Excel
                self._write_excel(non_empty_data, output_path)
            
            logger.info(f"Output generated: {output_path}")

        except Exception as e:
            logger.error(f"Error generating Excel output: {str(e)}")
            raise

    def _process_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Process data to match schema requirements."""
        processed = {}
        
        for tab_name in self.tab_order:
            if tab_name in data and tab_name in self.schema:
                df = data[tab_name]
                if not df.empty:
                    # Keep only columns defined in schema
                    schema_columns = list(self.schema[tab_name].keys())
                    existing_columns = [col for col in schema_columns if col in df.columns]
                    
                    # Use predefined column order if available
                    if tab_name in self.column_orders:
                        existing_columns = [col for col in self.column_orders[tab_name] if col in df.columns]
                    
                    processed[tab_name] = df[existing_columns].copy()
                else:
                    processed[tab_name] = df.copy()

        return processed

    def _write_excel(self, data: Dict[str, pd.DataFrame], output_path: str) -> None:
        """Write data to Excel file with correct tab order."""
        logger.info(f"Writing Excel file to {output_path}")
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            sheets_written = False
            for tab_name in self.tab_order:
                if tab_name in data:
                    df = data[tab_name]
                    logger.debug(f"Processing {tab_name} for writing:")
                    logger.debug(f"  Shape: {df.shape}")
                    logger.debug(f"  Empty: {df.empty}")
                    
                    if not df.empty:
                        # Ensure correct column order before writing
                        if tab_name in self.column_orders:
                            columns = [col for col in self.column_orders[tab_name] if col in df.columns]
                            df = df[columns]
                        
                        logger.info(f"Writing sheet {tab_name} with {df.shape[0]} rows and {df.shape[1]} columns")
                        df.to_excel(writer, sheet_name=tab_name, index=False)
                        sheets_written = True
                    else:
                        logger.debug(f"Skipping empty sheet {tab_name}")
            
            # If no sheets were written, create a dummy sheet
            if not sheets_written:
                logger.warning("No data sheets written, creating Info sheet")
                pd.DataFrame({'Message': ['No valid data found to export']}).to_excel(
                    writer, sheet_name='Info', index=False
                )
