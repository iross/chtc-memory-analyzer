"""Memory usage analysis"""

from collections import defaultdict
from typing import Any, Dict, List, Tuple

import pandas as pd

from .stats import calculate_stats


class MemoryAnalyzer:
    """Analyzer for memory usage patterns"""

    def __init__(self, min_jobs: int = 20):
        """
        Initialize Memory Analyzer.

        Args:
            min_jobs: Minimum number of jobs in a cluster to analyze
        """
        self.min_jobs = min_jobs

    def analyze(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Main analysis orchestrator.

        Args:
            df: DataFrame with job data. Must contain columns: ClusterId, Owner,
                RequestMemory, MemoryUsage

        Returns:
            Dict with keys: cluster_analyses, user_totals, over_allocators

        Raises:
            ValueError: If required columns are missing
        """
        # Validate required columns
        required = {"ClusterId", "Owner", "RequestMemory", "MemoryUsage"}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            raise ValueError(
                f"DataFrame missing required columns for memory analysis: {missing}. "
                f"Available columns: {set(df.columns)}"
            )

        # Filter clusters by min_jobs threshold
        cluster_counts = df.groupby("ClusterId").size()
        large_clusters = cluster_counts[cluster_counts >= self.min_jobs].index
        filtered_df = df[df["ClusterId"].isin(large_clusters)]

        # Analyze by cluster
        cluster_analyses = self._analyze_by_cluster(filtered_df)

        # Analyze by user
        user_totals = self._analyze_by_user(cluster_analyses)

        # Find over-allocators
        over_allocators = self._find_over_allocators(cluster_analyses)

        return {
            "cluster_analyses": cluster_analyses,
            "user_totals": user_totals,
            "over_allocators": over_allocators,
        }

    def _analyze_by_cluster(self, df: pd.DataFrame) -> List[Dict]:
        """
        Group by ClusterId and calculate memory stats.

        Args:
            df: DataFrame with job data

        Returns:
            List of cluster analysis dicts
        """
        analyses = []

        for cluster_id, group in df.groupby("ClusterId"):
            # Get owner (assume all jobs in cluster have same owner)
            owner = group["Owner"].iloc[0] if len(group) > 0 else "unknown"

            # Extract memory data
            requested_memory = group["RequestMemory"].tolist()
            used_memory = group["MemoryUsage"].tolist()

            # Calculate usage ratios
            memory_ratios = []
            for req, used in zip(requested_memory, used_memory):
                if req and req > 0 and used is not None:
                    memory_ratios.append(used / req)

            analysis = {
                "cluster_id": int(cluster_id),
                "job_count": len(group),
                "owner": owner,
                "memory": {
                    "requested": calculate_stats(requested_memory),
                    "used": calculate_stats(used_memory),
                    "ratios": calculate_stats(memory_ratios),
                },
                "raw_data": {"used_memory": used_memory},
            }
            analyses.append(analysis)

        # Sort by cluster_id
        analyses.sort(key=lambda x: x["cluster_id"])
        return analyses

    def _analyze_by_user(self, cluster_analyses: List[Dict]) -> Dict:
        """
        Aggregate by user across all clusters.

        Args:
            cluster_analyses: List of cluster analysis dicts

        Returns:
            Dict mapping owner to their totals
        """
        user_totals = defaultdict(
            lambda: {
                "clusters": [],
                "total_jobs": 0,
                "total_requested_memory": 0,
                "total_used_memory": 0,
                "memory_ratios": [],
            }
        )

        for analysis in cluster_analyses:
            owner = analysis["owner"]
            user_totals[owner]["clusters"].append(analysis["cluster_id"])
            user_totals[owner]["total_jobs"] += analysis["job_count"]

            # Sum up memory values
            mem_req_stats = analysis["memory"]["requested"]
            mem_use_stats = analysis["memory"]["used"]

            if mem_req_stats["count"] > 0:
                user_totals[owner]["total_requested_memory"] += (
                    mem_req_stats["mean"] * mem_req_stats["count"]
                )
            if mem_use_stats["count"] > 0:
                user_totals[owner]["total_used_memory"] += (
                    mem_use_stats["mean"] * mem_use_stats["count"]
                )

            # Collect memory ratios for averaging
            if analysis["memory"]["ratios"]["count"] > 0:
                user_totals[owner]["memory_ratios"].append(analysis["memory"]["ratios"]["mean"])

        return dict(user_totals)

    def _find_over_allocators(
        self, cluster_analyses: List[Dict], threshold: float = 0.5
    ) -> List[Tuple[int, str, float]]:
        """
        Find clusters using less than threshold of requested memory.

        Args:
            cluster_analyses: List of cluster analysis dicts
            threshold: Usage ratio threshold (default: 0.5 = 50%)

        Returns:
            List of tuples: (cluster_id, owner, usage_ratio), sorted by ratio
        """
        over_alloc = []

        for analysis in cluster_analyses:
            mem_ratio = analysis["memory"]["ratios"]["mean"]

            if mem_ratio > 0 and mem_ratio < threshold:
                over_alloc.append((analysis["cluster_id"], analysis["owner"], mem_ratio))

        # Sort by ratio (lowest first = worst over-allocators)
        over_alloc.sort(key=lambda x: x[2])
        return over_alloc
