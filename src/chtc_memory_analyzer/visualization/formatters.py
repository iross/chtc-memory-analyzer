"""Data formatting utilities"""

from typing import Any, Dict

from .histogram import create_histogram


def format_bytes(bytes_value):
    """Convert bytes to human-readable format"""
    if bytes_value is None:
        return "N/A"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_cluster_report(analysis: Dict) -> str:
    """Format cluster analysis as string for display"""
    cluster_id = analysis["cluster_id"]
    job_count = analysis["job_count"]
    owner = analysis["owner"]

    lines = []
    lines.append("\n" + "=" * 80)
    lines.append(f"Cluster: {cluster_id} | Owner: {owner} | Jobs: {job_count}")
    lines.append("=" * 80)

    # Memory Analysis
    lines.append(f"\nMEMORY REQUEST: {analysis.get('memory', {}).get('requested', {})}")
    lines.append("\nMEMORY USAGE:")
    lines.append("-" * 80)
    mem_use_stats = analysis["memory"]["used"]
    mem_ratio_stats = analysis["memory"]["ratios"]

    lines.append("  Used Memory (MB):")
    lines.append(
        "    Mean: {:.2f} | Median: {:.2f} | Std Dev: {:.2f}".format(
            mem_use_stats["mean"], mem_use_stats["median"], mem_use_stats["stdev"]
        )
    )
    lines.append("    Min: {:.2f} | Max: {:.2f}".format(mem_use_stats["min"], mem_use_stats["max"]))

    if mem_ratio_stats["count"] > 0:
        avg_ratio = mem_ratio_stats["mean"]
        lines.append("\n  Usage Ratio (Used/Requested):")
        lines.append(
            "    Mean: {:.2%} | Median: {:.2%}".format(avg_ratio, mem_ratio_stats["median"])
        )

    # Memory histogram
    if analysis["raw_data"]["used_memory"]:
        lines.append("\n  Memory Usage Histogram (MB):")
        lines.append(create_histogram(analysis["raw_data"]["used_memory"]))

    return "\n".join(lines)


def format_summary_report(results: Dict[str, Any]) -> str:
    """Format overall summary report"""
    import statistics

    lines = []
    analyses = results.get("cluster_analyses", [])
    user_totals = results.get("user_totals", {})
    over_allocators = results.get("over_allocators", [])

    # Summary header
    lines.append("\n\n" + "=" * 80)
    lines.append("SUMMARY")
    lines.append("=" * 80)
    lines.append(f"Total clusters analyzed: {len(analyses)}")

    # Collect all memory usage data across all clusters
    all_memory_usage = []
    for analysis in analyses:
        all_memory_usage.extend(analysis["raw_data"]["used_memory"])

    if all_memory_usage:
        lines.append("\n" + "=" * 80)
        lines.append("MEMORY USAGE HISTOGRAM - ALL CLUSTERS")
        lines.append("=" * 80)
        lines.append(f"Total jobs with memory data: {len(all_memory_usage)}")

        from ..analysis.stats import calculate_stats

        mem_stats = calculate_stats(all_memory_usage)
        lines.append(
            "Mean: {:.2f} MB | Median: {:.2f} MB | Std Dev: {:.2f} MB".format(
                mem_stats["mean"], mem_stats["median"], mem_stats["stdev"]
            )
        )
        lines.append("Min: {:.2f} MB | Max: {:.2f} MB".format(mem_stats["min"], mem_stats["max"]))
        lines.append("\nHistogram:")
        lines.append(create_histogram(all_memory_usage))

    # Per-user totals
    if user_totals:
        lines.append("\n" + "=" * 80)
        lines.append("PER-USER TOTALS ACROSS ALL CLUSTERS")
        lines.append("=" * 80)

        # Sort users by total jobs
        sorted_users = sorted(user_totals.items(), key=lambda x: x[1]["total_jobs"], reverse=True)

        for owner, totals in sorted_users:
            lines.append(f"\nUser: {owner}")
            cluster_ids = ", ".join(map(str, sorted(totals["clusters"])))
            lines.append(f"  Clusters: {len(totals['clusters'])} (IDs: {cluster_ids})")
            lines.append(f"  Total Jobs: {totals['total_jobs']}")
            req_mem_bytes = totals["total_requested_memory"] * 1024 * 1024
            lines.append(f"  Total Requested Memory: {format_bytes(req_mem_bytes)}")
            used_mem_bytes = totals["total_used_memory"] * 1024 * 1024
            lines.append(f"  Total Used Memory: {format_bytes(used_mem_bytes)}")

            if totals["memory_ratios"]:
                avg_ratio = statistics.mean(totals["memory_ratios"])
                lines.append(f"  Average Memory Usage Ratio: {avg_ratio:.2%}")

                if totals["total_requested_memory"] > 0:
                    overall_ratio = totals["total_used_memory"] / totals["total_requested_memory"]
                    lines.append(f"  Overall Memory Usage Ratio: {overall_ratio:.2%}")

    # Over-allocators
    if over_allocators:
        lines.append("\n" + "=" * 80)
        lines.append("Top Memory Over-Allocators (using <50% of requested):")
        lines.append("=" * 80)
        for cid, owner, ratio in over_allocators[:10]:
            lines.append(f"  Cluster {cid} ({owner}): {ratio:.1%} average usage")

    return "\n".join(lines)
