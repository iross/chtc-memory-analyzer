#!/usr/bin/env python
"""
HTCondor Resource Usage Analysis Script

This script analyzes HTCondor job history to identify:
- Clusters with more than 20 jobs
- Memory usage patterns and over-allocation

Usage:
    chtc-analyze [--schedd SCHEDD_NAME] [--min-jobs MIN_JOBS] [--csv FILE] [--cache-csv FILE]
    chtc-analyze --fetch-all --cache-csv all_data.csv  # Fetch all attributes
    chtc-analyze --attributes ClusterId,Owner,RequestCpus,RequestMemory  # Custom attributes
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
@click.option(
    "--attributes",
    help="Comma-separated list of HTCondor attributes to fetch (e.g., ClusterId,Owner)",
)
@click.option(
    "--fetch-all",
    is_flag=True,
    help="Fetch all available job attributes (exploratory mode, overrides --attributes)",
)
def main(schedd, min_jobs, limit, constraint, csv, cache_csv, attributes, fetch_all):
    """
    Analyze HTCondor job history for memory usage patterns.

    Identifies clusters with many jobs and analyzes their memory usage
    to find over-allocation.

    Examples:

        # Default: fetch core memory analysis attributes
        chtc-analyze --min-jobs 20

        # Fetch all attributes for exploration
        chtc-analyze --fetch-all --cache-csv all_jobs.csv

        # Fetch specific attributes for efficient queries
        chtc-analyze --attributes "ClusterId,Owner,RequestCpus,RequestMemory,MemoryUsage"
    """

    if fetch_all:
        print("Fetching ALL job attributes (exploratory mode)...")
    elif attributes:
        print(f"Fetching custom attributes: {attributes}")
    else:
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

        # Parse attributes if provided
        attr_list = None
        if attributes:
            attr_list = [a.strip() for a in attributes.split(",")]

        try:
            df = source.fetch_jobs(
                schedd=schedd,
                constraint=constraint or "JobStatus == 4",
                match_limit=10000,
                attributes=attr_list,
                fetch_all=fetch_all,
            )
        except Exception as e:
            print(f"Error querying history: {e}")
            return

        # Optionally cache to CSV
        if cache_csv:
            print(f"Caching results to CSV: {cache_csv}")
            df.to_csv(cache_csv, index=False)
            print(f"Cached {len(df)} jobs with {len(df.columns)} attributes")

            if fetch_all:
                print(f"\nAvailable columns: {list(df.columns)}")
                print("\nExplore the data with:")
                print("  import pandas as pd")
                print(f"  df = pd.read_csv('{cache_csv}')")
                print("  print(df.columns)")
                return

    # Check if we have required columns for memory analysis
    required_columns = {"ClusterId", "Owner", "RequestMemory", "MemoryUsage"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        print("\nWarning: Cannot run memory analysis.")
        print(f"Missing required columns: {missing}")
        print(f"Available columns: {list(df.columns)}")
        print("\nTo run memory analysis, ensure these columns are present")
        print("or fetch default attributes.")
        return

    print(f"Found {len(df['ClusterId'].unique())} unique clusters")

    # Analysis Layer
    analyzer = MemoryAnalyzer(min_jobs=min_jobs)
    try:
        results = analyzer.analyze(df)
    except ValueError as e:
        print(f"\nError during analysis: {e}")
        return

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
