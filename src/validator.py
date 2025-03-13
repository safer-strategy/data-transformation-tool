import pandas as pd
import logging
import json
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class Validator:
    """Validates transformed data against schema rules."""

    def __init__(self, schema_path: str):
        """Initialize validator with schema.

        Args:
            schema_path: Path to the schema JSON file
        """
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

    def validate_data(self, data: Dict[str, pd.DataFrame]) -> None:
        """Validate transformed data against schema rules.

        Args:
            data: Dictionary mapping tab names to DataFrames

        Raises:
            ValueError: If validation fails
        """
        for tab_name, df in data.items():
            logger.debug(f"Validating tab: {tab_name}")
            
            if tab_name == "Users":
                self._validate_users_tab(df)
            elif tab_name in [
                "User Groups",
                "User Roles",
                "Group Roles",
                "User Resources",
                "Role Resources",
                "Group Groups"
            ]:
                self._validate_relationship_tab(df, tab_name)

    def _validate_users_tab(self, df: pd.DataFrame) -> None:
        """Validate Users tab specific rules.

        Args:
            df: DataFrame containing user data

        Raises:
            ValueError: If validation fails
        """
        # Check if at least one identifier is present for each row
        identifiers = ['user_id', 'username', 'email']
        present_identifiers = [col for col in identifiers if col in df.columns]
        
        if not present_identifiers:
            raise ValueError(
                "Users tab must contain at least one identifier column "
                "(user_id, username, or email)"
            )
        
        # Check if each row has at least one identifier
        has_identifier = df[present_identifiers].notna().any(axis=1)
        missing_ids = df[~has_identifier].index.tolist()
        
        if missing_ids:
            raise ValueError(
                f"Users at indices {missing_ids} are missing all identifiers "
                f"({', '.join(present_identifiers)})"
            )

    def _validate_relationship_tab(
        self,
        df: pd.DataFrame,
        tab_name: str
    ) -> None:
        """Validate relationship tab rules.

        Args:
            df: DataFrame containing relationship data
            tab_name: Name of the relationship tab

        Raises:
            ValueError: If validation fails
        """
        required_columns = self._get_required_columns(tab_name)
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(
                f"{tab_name} is missing required columns: {missing_columns}"
            )
        
        # Check for null values in required columns
        for col in required_columns:
            null_indices = df[df[col].isna()].index.tolist()
            if null_indices:
                raise ValueError(
                    f"{tab_name} has null values in {col} at indices: {null_indices}"
                )

    def _get_required_columns(self, tab_name: str) -> List[str]:
        """Get required columns for a relationship tab.

        Args:
            tab_name: Name of the relationship tab

        Returns:
            List[str]: List of required column names
        """
        required_columns = {
            "User Groups": ['user_id', 'group_id'],
            "User Roles": ['user_id', 'role_id'],
            "Group Roles": ['group_id', 'role_id'],
            "User Resources": ['user_id', 'resource_id'],
            "Role Resources": ['role_id', 'resource_id'],
            "Group Groups": ['parent_group_id', 'child_group_id']
        }
        return required_columns.get(tab_name, [])
