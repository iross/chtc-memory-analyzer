"""CSV data source"""

from typing import Dict, Optional

import pandas as pd

from .base import DataSource


class CSVSource(DataSource):
    """Fetch job data from CSV files"""

    def fetch_jobs(
        self, filepath: str, column_mapping: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """
        Read job data from CSV file and return as DataFrame.

        Args:
            filepath: Path to CSV file
            column_mapping: Optional dict mapping CSV columns to standard schema
                          e.g., {'ClusterId': 'cluster_id', 'RequestMemory': 'request_memory_mb'}

        Returns:
            DataFrame with standardized schema
        """
        # Read CSV
        df = pd.read_csv(filepath)

        # Apply column mapping if provided
        if column_mapping:
            df = df.rename(columns=column_mapping)

        # Ensure correct dtypes
        df["cluster_id"] = df["cluster_id"].astype(int)
        df["proc_id"] = df["proc_id"].astype(int)
        df["owner"] = df["owner"].astype(str)
        df["request_memory_mb"] = df["request_memory_mb"].astype(float)
        df["used_memory_mb"] = df["used_memory_mb"].astype(float)
        df["job_status"] = df["job_status"].astype(int)

        # Validate schema
        if not self.validate_dataframe(df):
            required_cols = (
                "cluster_id, proc_id, owner, "
                "request_memory_mb, used_memory_mb, job_status"
            )
            raise ValueError(
                f"CSV file missing required columns. "
                f"Required: {required_cols}. "
                f"Found: {list(df.columns)}"
            )

        return df
