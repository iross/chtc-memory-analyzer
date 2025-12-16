"""Base data source interface"""

from abc import ABC, abstractmethod
from typing import Optional, Set

import pandas as pd

# Common column sets for different use cases
MEMORY_ANALYSIS_COLUMNS = {
    "ClusterId",
    "ProcId",
    "Owner",
    "RequestMemory",
    "MemoryUsage",
    "JobStatus",
}

CPU_ANALYSIS_COLUMNS = {
    "ClusterId",
    "ProcId",
    "Owner",
    "RequestCpus",
    "CpusUsage",
    "JobStatus",
}

CORE_COLUMNS = {"ClusterId", "ProcId", "Owner", "JobStatus"}


class DataSource(ABC):
    """Abstract base class for job data sources."""

    @abstractmethod
    def fetch_jobs(self, **kwargs) -> pd.DataFrame:
        """
        Fetch job data and return as DataFrame.

        Returns:
            DataFrame with job data. Column names depend on the data source
            and parameters used (e.g., HTCondor attribute names).
        """
        pass

    @classmethod
    def validate_dataframe(
        cls, df: pd.DataFrame, required_columns: Optional[Set[str]] = None
    ) -> bool:
        """
        Validate DataFrame has required columns.

        Args:
            df: DataFrame to validate
            required_columns: Optional set of required columns. If None, no validation.

        Returns:
            True if valid or no validation needed
        """
        if required_columns is None:
            return True  # No validation needed

        return required_columns.issubset(df.columns)
