"""HTCondor data source"""

from typing import List, Optional

import pandas as pd

from .base import DataSource

try:
    import classad2 as classad
    import htcondor2 as htcondor

    HTCONDOR_AVAILABLE = True
except ImportError:
    HTCONDOR_AVAILABLE = False
    classad = None
    htcondor = None


class HTCondorSource(DataSource):
    """Fetch job data from HTCondor history API"""

    def fetch_jobs(
        self,
        schedd: Optional[str] = None,
        constraint: str = "JobStatus == 4",
        match_limit: int = 10000,
        attributes: Optional[List[str]] = None,
        fetch_all: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch job data from HTCondor and return as DataFrame.

        Args:
            schedd: Schedd name to query (default: local schedd)
            constraint: HTCondor constraint expression (default: completed jobs only)
            match_limit: Maximum number of jobs to fetch (default: 10000)
            attributes: List of specific HTCondor attributes to fetch (e.g., ['ClusterId', 'Owner'])
            fetch_all: If True, fetch all available attributes (overrides attributes parameter)

        Returns:
            DataFrame with job data. Column names are HTCondor attribute names.

        Examples:
            # Default: fetch core memory analysis attributes
            df = source.fetch_jobs(schedd="my-schedd")

            # Fetch all attributes (exploratory mode)
            df = source.fetch_jobs(schedd="my-schedd", fetch_all=True)

            # Fetch specific attributes (efficient mode)
            df = source.fetch_jobs(
                schedd="my-schedd",
                attributes=['ClusterId', 'Owner', 'RequestCpus', 'RequestMemory']
            )
        """
        if not HTCONDOR_AVAILABLE:
            raise ImportError(
                "HTCondor is not available. Install with: uv pip install -e '.[htcondor]'"
            )

        # Connect to schedd
        if schedd:
            schedd_obj = htcondor.Schedd(schedd)
        else:
            schedd_obj = htcondor.Schedd()

        # Determine projection based on mode
        if fetch_all:
            projection = []  # Empty list = fetch all attributes
        elif attributes:
            projection = attributes
        else:
            # Default: core attributes for memory analysis
            projection = [
                "ClusterId",
                "ProcId",
                "Owner",
                "RequestMemory",
                "MemoryUsage",
                "JobStatus",
            ]

        # Query history
        try:
            ads = schedd_obj.history(
                constraint=constraint, projection=projection, match=match_limit
            )
        except Exception as e:
            raise RuntimeError(f"Error querying HTCondor history: {e}")

        # Transform ClassAds to records
        records = []
        for ad in ads:
            record = {}

            # If we have a projection, use it; otherwise iterate all keys
            keys_to_process = projection if projection else ad.keys()

            for key in keys_to_process:
                value = ad.get(key)

                # Handle ExprTree evaluation
                if value is not None and isinstance(value, classad._expr_tree.ExprTree):
                    try:
                        value = value.eval()
                    except Exception:
                        # If evaluation fails, convert to string representation
                        value = str(value)

                record[key] = value

            records.append(record)

        # Create DataFrame
        df = pd.DataFrame(records)

        return df
