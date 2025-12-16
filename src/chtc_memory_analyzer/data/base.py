"""Base data source interface"""

from abc import ABC, abstractmethod

import pandas as pd


class DataSource(ABC):
    """Abstract base class for job data sources."""

    @abstractmethod
    def fetch_jobs(self, **kwargs) -> pd.DataFrame:
        """
        Fetch job data and return as standardized DataFrame.

        Returns:
            DataFrame with columns: cluster_id, proc_id, owner,
                                   request_memory_mb, used_memory_mb, job_status
        """
        pass

    @classmethod
    def validate_dataframe(cls, df: pd.DataFrame) -> bool:
        """Validate DataFrame has required columns."""
        required = {
            "cluster_id",
            "proc_id",
            "owner",
            "request_memory_mb",
            "used_memory_mb",
            "job_status",
        }
        return required.issubset(df.columns)
