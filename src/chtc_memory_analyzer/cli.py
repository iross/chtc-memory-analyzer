#!/usr/bin/env python
"""
HTCondor Resource Usage Analysis Script

This script analyzes HTCondor job history to identify:
- Clusters with more than 20 jobs
- Memory usage patterns and over-allocation

Usage:
    python resource_usage_analysis.py [--schedd SCHEDD_NAME] [--min-jobs MIN_JOBS]
"""

import htcondor2 as htcondor
import classad2 as classad
import click
import statistics
from collections import defaultdict
from typing import Dict, List


def format_bytes(bytes_value):
    """Convert bytes to human-readable format"""
    if bytes_value is None:
        return "N/A"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def create_histogram(values: List[float], bins: int = 10, width: int = 50) -> str:
    """Create a simple ASCII histogram"""
    if not values:
        return "No data"

    min_val = min(values)
    max_val = max(values)

    if min_val == max_val:
        return f"All values equal: {min_val:.2f}"

    # Create bins
    bin_width = (max_val - min_val) / bins
    bin_counts = [0] * bins

    for val in values:
        bin_idx = min(int((val - min_val) / bin_width), bins - 1)
        bin_counts[bin_idx] += 1

    # Find max count for scaling
    max_count = max(bin_counts)

    # Build histogram
    histogram = []
    for i, count in enumerate(bin_counts):
        bin_start = min_val + i * bin_width
        bin_end = bin_start + bin_width
        bar_length = int((count / max_count) * width) if max_count > 0 else 0
        bar = '█' * bar_length
        histogram.append(f"  {bin_start:8.2f} - {bin_end:8.2f} | {bar} ({count})")

    return '\n'.join(histogram)


def calculate_stats(values: List[float]) -> Dict[str, float]:
    """Calculate statistical measures for a list of values"""
    if not values:
        return {
            'mean': 0,
            'median': 0,
            'stdev': 0,
            'min': 0,
            'max': 0,
            'count': 0
        }

    return {
        'mean': statistics.mean(values),
        'median': statistics.median(values),
        'stdev': statistics.stdev(values) if len(values) > 1 else 0,
        'min': min(values),
        'max': max(values),
        'count': len(values)
    }


def analyze_cluster_jobs(jobs: List[dict], cluster_id: int) -> Dict:
    """Analyze memory usage for jobs in a cluster"""

    # Extract memory data
    requested_memory = []
    used_memory = []

    for job in jobs:
        # Memory (in MB)
        req_mem = job.get('RequestMemory', 0)
        # MemoryUsage is typically in MB
        use_mem = job.get('MemoryUsage', 0)
        if isinstance(use_mem, classad._expr_tree.ExprTree):
            use_mem = use_mem.eval()

        if req_mem > 0:
            requested_memory.append(req_mem)
        if use_mem > 0:
            used_memory.append(use_mem)

    # Calculate usage ratios (how much of requested was actually used)
    memory_ratios = []
    for i in range(min(len(requested_memory), len(used_memory))):
        if requested_memory[i] > 0:
            memory_ratios.append(used_memory[i] / requested_memory[i])

    return {
        'cluster_id': cluster_id,
        'job_count': len(jobs),
        'owner': jobs[0].get('Owner', 'unknown') if jobs else 'unknown',
        'memory': {
            'requested': calculate_stats(requested_memory),
            'used': calculate_stats(used_memory),
            'ratios': calculate_stats(memory_ratios)
        },
        'raw_data': {
            'used_memory': used_memory
        }
    }


def print_cluster_analysis(analysis: Dict):
    """Pretty print the analysis for a cluster"""
    cluster_id = analysis['cluster_id']
    job_count = analysis['job_count']
    owner = analysis['owner']

    print("\n" + "="*80)
    print("Cluster: {} | Owner: {} | Jobs: {}".format(cluster_id, owner, job_count))
    print("="*80)

    # Memory Analysis
    print(f"\nMEMORY REQUEST: {analysis.get('memory', {}).get('requested', {})}")
    print("\nMEMORY USAGE:")
    print("-" * 80)
    mem_use_stats = analysis['memory']['used']
    mem_ratio_stats = analysis['memory']['ratios']

    print("  Used Memory (MB):")
    print("    Mean: {:.2f} | Median: {:.2f} | Std Dev: {:.2f}".format(
        mem_use_stats['mean'], mem_use_stats['median'], mem_use_stats['stdev']))
    print("    Min: {:.2f} | Max: {:.2f}".format(
        mem_use_stats['min'], mem_use_stats['max']))

    if mem_ratio_stats['count'] > 0:
        avg_ratio = mem_ratio_stats['mean']
        print("\n  Usage Ratio (Used/Requested):")
        print("    Mean: {:.2%} | Median: {:.2%}".format(avg_ratio, mem_ratio_stats['median']))

    # Memory histogram
    if analysis['raw_data']['used_memory']:
        print("\n  Memory Usage Histogram (MB):")
        print(create_histogram(analysis['raw_data']['used_memory']))


@click.command()
@click.option('--schedd', default=None, help='Schedd name to query (default: local schedd)')
@click.option('--min-jobs', default=20, help='Minimum number of jobs in a cluster to analyze')
@click.option('--limit', default=100, help='Maximum number of clusters to analyze')
@click.option('--constraint', default=None, help='Additional constraint for history query')
def main(schedd, min_jobs, limit, constraint):
    """
    Analyze HTCondor job history for memory usage patterns.

    Identifies clusters with many jobs and analyzes their memory usage
    to find over-allocation.
    """

    print("Querying HTCondor history for clusters with >{} jobs...".format(min_jobs))
    print("="*80 + "\n")

    # Connect to schedd
    if schedd:
        schedd_obj = htcondor.Schedd(schedd)
    else:
        schedd_obj = htcondor.Schedd()

    # Build constraint
    base_constraint = "JobStatus == 4"  # Completed jobs
    if constraint:
        base_constraint = f"{base_constraint} && ({constraint})"

    # Query history
    projection = [
        'ClusterId', 'ProcId', 'Owner',
        'RequestMemory', 'MemoryUsage',
        'JobStatus'
    ]

    print("Fetching job history (this may take a while)...")
    try:
        ads = schedd_obj.history(
            constraint=base_constraint,
            projection=projection,
            match=10000  # Fetch up to 10k jobs
        )
    except Exception as e:
        print("Error querying history: {}".format(e))
        return

    # Group jobs by cluster
    clusters = defaultdict(list)
    for ad in ads:
        cluster_id = ad.get('ClusterId')
        if cluster_id is not None:
            clusters[cluster_id].append(dict(ad))

    print("Found {} unique clusters".format(len(clusters)))

    # Filter clusters with minimum job count
    large_clusters = {cid: jobs for cid, jobs in clusters.items() if len(jobs) >= min_jobs}

    print("Found {} clusters with >={} jobs\n".format(len(large_clusters), min_jobs))

    if not large_clusters:
        print("No clusters found matching criteria.")
        return

    # Analyze each cluster
    analyses = []
    for cluster_id, jobs in sorted(large_clusters.items())[:limit]:
        analysis = analyze_cluster_jobs(jobs, cluster_id)
        analyses.append(analysis)
        print_cluster_analysis(analysis)

    # Summary
    print("\n\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("Total clusters analyzed: {}".format(len(analyses)))

    # Collect all memory usage data across all clusters
    all_memory_usage = []
    for analysis in analyses:
        all_memory_usage.extend(analysis['raw_data']['used_memory'])

    if all_memory_usage:
        print("\n" + "="*80)
        print("MEMORY USAGE HISTOGRAM - ALL CLUSTERS")
        print("="*80)
        print(f"Total jobs with memory data: {len(all_memory_usage)}")
        mem_stats = calculate_stats(all_memory_usage)
        print("Mean: {:.2f} MB | Median: {:.2f} MB | Std Dev: {:.2f} MB".format(
            mem_stats['mean'], mem_stats['median'], mem_stats['stdev']))
        print("Min: {:.2f} MB | Max: {:.2f} MB".format(
            mem_stats['min'], mem_stats['max']))
        print("\nHistogram:")
        print(create_histogram(all_memory_usage))

    # Per-user totals across all clusters
    user_totals = defaultdict(lambda: {
        'clusters': [],
        'total_jobs': 0,
        'total_requested_memory': 0,
        'total_used_memory': 0,
        'memory_ratios': []
    })

    for analysis in analyses:
        owner = analysis['owner']
        user_totals[owner]['clusters'].append(analysis['cluster_id'])
        user_totals[owner]['total_jobs'] += analysis['job_count']

        # Sum up memory values
        mem_req_stats = analysis['memory']['requested']
        mem_use_stats = analysis['memory']['used']

        if mem_req_stats['count'] > 0:
            user_totals[owner]['total_requested_memory'] += mem_req_stats['mean'] * mem_req_stats['count']
        if mem_use_stats['count'] > 0:
            user_totals[owner]['total_used_memory'] += mem_use_stats['mean'] * mem_use_stats['count']

        # Collect memory ratios for averaging
        if analysis['memory']['ratios']['count'] > 0:
            user_totals[owner]['memory_ratios'].append(analysis['memory']['ratios']['mean'])

    print("\n" + "="*80)
    print("PER-USER TOTALS ACROSS ALL CLUSTERS")
    print("="*80)

    # Sort users by total jobs
    sorted_users = sorted(user_totals.items(), key=lambda x: x[1]['total_jobs'], reverse=True)

    for owner, totals in sorted_users:
        print(f"\nUser: {owner}")
        print(f"  Clusters: {len(totals['clusters'])} (IDs: {', '.join(map(str, sorted(totals['clusters'])))})")
        print(f"  Total Jobs: {totals['total_jobs']}")
        print(f"  Total Requested Memory: {format_bytes(totals['total_requested_memory'] * 1024 * 1024)}")
        print(f"  Total Used Memory: {format_bytes(totals['total_used_memory'] * 1024 * 1024)}")

        if totals['memory_ratios']:
            avg_ratio = statistics.mean(totals['memory_ratios'])
            print(f"  Average Memory Usage Ratio: {avg_ratio:.2%}")

            if totals['total_requested_memory'] > 0:
                overall_ratio = totals['total_used_memory'] / totals['total_requested_memory']
                print(f"  Overall Memory Usage Ratio: {overall_ratio:.2%}")

    # Find worst over-allocators
    mem_over_alloc = []

    for analysis in analyses:
        mem_ratio = analysis['memory']['ratios']['mean']

        if mem_ratio > 0 and mem_ratio < 0.5:
            mem_over_alloc.append((analysis['cluster_id'], analysis['owner'], mem_ratio))

    if mem_over_alloc:
        print("\n" + "="*80)
        print("Top Memory Over-Allocators (using <50% of requested):")
        print("="*80)
        for cid, owner, ratio in sorted(mem_over_alloc, key=lambda x: x[2])[:10]:
            print("  Cluster {} ({}): {:.1%} average usage".format(cid, owner, ratio))


if __name__ == '__main__':
    main()
