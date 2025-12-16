"""Elasticsearch data source (stub for future implementation)"""

from typing import Dict, List, Optional

import pandas as pd

from .base import DataSource


class ElasticsearchSource(DataSource):
    """Fetch job data from Elasticsearch (future implementation)"""

    def fetch_jobs(
        self, hosts: List[str], index: str, query: Optional[Dict] = None, **kwargs
    ) -> pd.DataFrame:
        """
        Fetch job data from Elasticsearch and return as DataFrame.

        Args:
            hosts: List of Elasticsearch host URLs
            index: Elasticsearch index name
            query: Optional Elasticsearch query DSL dict
            **kwargs: Additional Elasticsearch client parameters

        Returns:
            DataFrame with standardized schema

        Raises:
            NotImplementedError: This feature is not yet implemented
        """
        raise NotImplementedError(
            "Elasticsearch support is planned but not yet implemented. "
            "Please use HTCondorSource or CSVSource for now."
        )
