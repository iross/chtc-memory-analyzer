#!/usr/bin/env python
"""
HTCondor Resource Usage Analysis Script

This script analyzes HTCondor job history to identify:
- Clusters with more than 20 jobs
- Memory usage patterns and over-allocation

Usage:
    chtc-analyze [--schedd SCHEDD_NAME] [--min-jobs MIN_JOBS] [--csv FILE] [--cache-csv FILE]
"""

import click

from chtc_memory_analyzer.analysis import MemoryAnalyzer
from chtc_memory_analyzer.data import CSVSource, HTCondorSource
from chtc_memory_analyzer.visualization import format_cluster_report, format_summary_report


@click.command()
@click.option("--schedd", default=None, help="Schedd name to query (default: local schedd)")
@click.option("--min-jobs", default=20, help="Minimum number of jobs in a cluster to analyze")
@click.option("--limit", default=100, help="Maximum number of clusters to analyze")
@click.option("--constraint", default=None, help="Additional constraint for history query")
@click.option("--csv", type=click.Path(exists=True), help="Read from CSV instead of HTCondor")
@click.option("--cache-csv", type=click.Path(), help="Cache HTCondor results to CSV")
def main(schedd, min_jobs, limit, constraint, csv, cache_csv):
    """
    Analyze HTCondor job history for memory usage patterns.

    Identifies clusters with many jobs and analyzes their memory usage
    to find over-allocation.
    """

    print(f"Querying job history for clusters with >{min_jobs} jobs...")
    print("=" * 80 + "\n")

    # Data Layer: Choose source and fetch data
    if csv:
        print(f"Reading job data from CSV: {csv}")
        source = CSVSource()
        df = source.fetch_jobs(filepath=csv)
    else:
        print("Fetching job history from HTCondor (this may take a while)...")
        source = HTCondorSource()
        try:
            df = source.fetch_jobs(
                schedd=schedd, constraint=constraint or "JobStatus == 4", match_limit=10000
            )
        except Exception as e:
            print(f"Error querying history: {e}")
            return

        # Optionally cache to CSV
        if cache_csv:
            print(f"Caching results to CSV: {cache_csv}")
            df.to_csv(cache_csv, index=False)

    print(f"Found {len(df['cluster_id'].unique())} unique clusters")

    # Analysis Layer
    analyzer = MemoryAnalyzer(min_jobs=min_jobs)
    results = analyzer.analyze(df)

    cluster_analyses = results["cluster_analyses"]
    print(f"Found {len(cluster_analyses)} clusters with >={min_jobs} jobs\n")

    if not cluster_analyses:
        print("No clusters found matching criteria.")
        return

    # Presentation Layer: Display results
    for analysis in cluster_analyses[:limit]:
        print(format_cluster_report(analysis))

    # Display summary
    print(format_summary_report(results))


if __name__ == "__main__":
    main()
