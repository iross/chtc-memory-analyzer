"""CSV data source"""

from typing import Dict, Optional, Set

import pandas as pd

from .base import DataSource


class CSVSource(DataSource):
    """Fetch job data from CSV files"""

    def fetch_jobs(
        self,
        filepath: str,
        column_mapping: Optional[Dict[str, str]] = None,
        validate_columns: Optional[Set[str]] = None,
    ) -> pd.DataFrame:
        """
        Read job data from CSV file and return as DataFrame.

        Args:
            filepath: Path to CSV file
            column_mapping: Optional dict mapping CSV columns to desired names
                          e.g., {'cluster_id': 'ClusterId'}
            validate_columns: Optional set of columns to validate. If None, no validation.

        Returns:
            DataFrame with job data

        Examples:
            # Read CSV with HTCondor column names
            df = source.fetch_jobs("jobs.csv")

            # Read CSV with custom column names, map to HTCondor names
            df = source.fetch_jobs(
                "jobs.csv",
                column_mapping={'cluster': 'ClusterId', 'user': 'Owner'}
            )

            # Read and validate specific columns are present
            from chtc_memory_analyzer.data.base import MEMORY_ANALYSIS_COLUMNS
            df = source.fetch_jobs("jobs.csv", validate_columns=MEMORY_ANALYSIS_COLUMNS)
        """
        # Read CSV
        df = pd.read_csv(filepath)

        # Apply column mapping if provided
        if column_mapping:
            df = df.rename(columns=column_mapping)

        # Optional validation
        if validate_columns and not self.validate_dataframe(df, validate_columns):
            missing = validate_columns - set(df.columns)
            raise ValueError(
                f"CSV file missing required columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        return df
