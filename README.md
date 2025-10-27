# CHTC Memory Analyzer

Analyze HTCondor job history for memory usage patterns and identify resource over-allocation.

## Features

- Identifies clusters with large numbers of jobs
- Analyzes memory usage patterns across jobs
- Detects memory over-allocation (jobs requesting more than they use)
- Provides per-user totals across all their clusters
- Generates ASCII histograms for memory usage distribution
- Statistical analysis (mean, median, standard deviation)

## Installation

### From PyPI (once published)

```bash
pip install chtc-memory-analyzer
```

### From source

```bash
git clone https://github.com/yourusername/chtc-memory-analyzer.git
cd chtc-memory-analyzer
pip install -e .
```

## Usage

Basic usage:

```bash
chtc-analyze
```

With options:

```bash
# Analyze clusters with at least 50 jobs
chtc-analyze --min-jobs 50

# Query specific schedd
chtc-analyze --schedd my-schedd.example.com

# Limit analysis to 20 clusters
chtc-analyze --limit 20

# Add custom constraint
chtc-analyze --constraint '''Owner == "user@example.com"'''
```

## Output

The tool provides:

1. **Per-cluster analysis**: Detailed memory statistics for each cluster
2. **Overall memory histogram**: Distribution across all analyzed jobs
3. **Per-user totals**: Aggregated statistics for each user across all their clusters
4. **Over-allocation report**: Identifies clusters using <50% of requested memory

## Requirements

- Python >= 3.8
- HTCondor Python bindings (htcondor >= 24.0.0)
- Click >= 8.0.0

## License

MIT License

## Contributing

Contributions welcome! Please open an issue or pull request.
