"""HTCondor data source"""

from typing import Optional

import classad2 as classad
import htcondor2 as htcondor
import pandas as pd

from .base import DataSource


class HTCondorSource(DataSource):
    """Fetch job data from HTCondor history API"""

    def fetch_jobs(
        self,
        schedd: Optional[str] = None,
        constraint: str = "JobStatus == 4",
        match_limit: int = 10000,
    ) -> pd.DataFrame:
        """
        Fetch job data from HTCondor and return as DataFrame.

        Args:
            schedd: Schedd name to query (default: local schedd)
            constraint: HTCondor constraint expression (default: completed jobs only)
            match_limit: Maximum number of jobs to fetch (default: 10000)

        Returns:
            DataFrame with standardized schema
        """
        # Connect to schedd
        if schedd:
            schedd_obj = htcondor.Schedd(schedd)
        else:
            schedd_obj = htcondor.Schedd()

        # Query history
        projection = ["ClusterId", "ProcId", "Owner", "RequestMemory", "MemoryUsage", "JobStatus"]

        try:
            ads = schedd_obj.history(
                constraint=constraint, projection=projection, match=match_limit
            )
        except Exception as e:
            raise RuntimeError(f"Error querying HTCondor history: {e}")

        # Transform ClassAds to records
        records = []
        for ad in ads:
            # Handle MemoryUsage which can be an ExprTree
            memory_usage = ad.get("MemoryUsage", 0)
            if isinstance(memory_usage, classad._expr_tree.ExprTree):
                memory_usage = memory_usage.eval()

            record = {
                "cluster_id": ad.get("ClusterId"),
                "proc_id": ad.get("ProcId"),
                "owner": ad.get("Owner", "unknown"),
                "request_memory_mb": float(ad.get("RequestMemory", 0)),
                "used_memory_mb": float(memory_usage) if memory_usage else 0.0,
                "job_status": ad.get("JobStatus"),
            }
            records.append(record)

        # Create DataFrame
        df = pd.DataFrame(records)

        # Validate schema
        if not self.validate_dataframe(df):
            raise ValueError("DataFrame missing required columns")

        return df
