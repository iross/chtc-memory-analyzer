# CHTC Memory Analyzer

Analyze HTCondor job history for memory usage patterns and identify resource over-allocation.

## Features

- **Flexible attribute fetching**: Fetch all HTCondor attributes or only what you need
- **Memory analysis**: Detect memory over-allocation and usage patterns
- **Exploratory mode**: Cache all job attributes for later analysis
- **Efficient queries**: Specify exactly which attributes to fetch
- **CSV caching**: Save and reload data for offline analysis
- **Statistical analysis**: Mean, median, standard deviation, histograms
- **Multi-cluster analysis**: Analyze patterns across many job clusters

## Installation

### From PyPI

**On systems with HTCondor (Linux):**
```bash
pip install chtc-memory-analyzer[htcondor]
```

**For development without HTCondor (macOS, Windows):**
```bash
pip install chtc-memory-analyzer
# HTCondor functionality will not be available, but you can still use CSV data sources
```

### From source

**With HTCondor:**
```bash
git clone https://github.com/iross/chtc-memory-analyzer.git
cd chtc-memory-analyzer
uv pip install -e ".[htcondor,dev,test]"
```

**Without HTCondor (development on macOS):**
```bash
git clone https://github.com/iross/chtc-memory-analyzer.git
cd chtc-memory-analyzer
uv pip install -e ".[dev,test]"
```

## Usage

### Query HTCondor directly

```bash
# Basic usage (requires HTCondor)
chtc-analyze

# Query specific schedd
chtc-analyze --schedd my-schedd.example.com

# Analyze clusters with at least 50 jobs
chtc-analyze --min-jobs 50

# Limit analysis to 20 clusters
chtc-analyze --limit 20

# Add custom constraint
chtc-analyze --constraint 'Owner == "user@example.com"'
```

### Flexible Attribute Fetching

**NEW in v0.4.0**: Choose what job attributes to fetch!

#### Exploratory Mode (Fetch All Attributes)

When you don't know what you're looking for, fetch everything:

```bash
# Fetch ALL available HTCondor attributes and cache to CSV
chtc-analyze --fetch-all --cache-csv all_jobs.csv

# Output shows what's available:
# Available columns: ['ClusterId', 'ProcId', 'Owner', 'RequestMemory',
#                    'MemoryUsage', 'RequestCpus', 'CpusUsage', 'JobStartDate',
#                    'JobStatus', 'RemoteHost', ...]
```

Then explore the data:
```python
import pandas as pd
df = pd.read_csv('all_jobs.csv')
print(df.columns)  # See what's available
print(df['RequestCpus'].describe())  # Explore CPU requests
```

#### Efficient Mode (Custom Projection)

When you know exactly what you need, fetch only those attributes:

```bash
# Fetch specific attributes for efficient queries
chtc-analyze --attributes "ClusterId,Owner,RequestCpus,RequestMemory,MemoryUsage,CpusUsage"

# Or save custom data for later
chtc-analyze --attributes "ClusterId,Owner,RequestDisk,DiskUsage" --cache-csv disk_usage.csv
```

#### Default Mode (Memory Analysis)

Without any flags, fetches only the core attributes needed for memory analysis:
- `ClusterId`, `ProcId`, `Owner`
- `RequestMemory`, `MemoryUsage`, `JobStatus`

```bash
# Default: memory analysis attributes only
chtc-analyze --min-jobs 20
```

### CSV Caching (for faster iteration)

```bash
# Cache HTCondor query results to CSV
chtc-analyze --cache-csv jobs.csv

# Later, analyze from CSV (no HTCondor needed!)
chtc-analyze --csv jobs.csv --min-jobs 30
```

### Library Usage

The refactored architecture makes it easy to use as a Python library:

```python
from chtc_memory_analyzer.data import HTCondorSource, CSVSource
from chtc_memory_analyzer.analysis import MemoryAnalyzer

# Method 1: Fetch all attributes (exploratory)
source = HTCondorSource()
df_all = source.fetch_jobs(schedd="my-schedd", fetch_all=True)

# Save for later exploration
df_all.to_csv("all_data.csv", index=False)
print(df_all.columns)  # See everything available

# Method 2: Fetch specific attributes (efficient)
df_custom = source.fetch_jobs(
    schedd="my-schedd",
    attributes=['ClusterId', 'Owner', 'RequestCpus', 'RequestMemory', 'MemoryUsage']
)

# Method 3: Default memory analysis attributes
df_memory = source.fetch_jobs(schedd="my-schedd")

# Run memory analysis
analyzer = MemoryAnalyzer(min_jobs=20)
results = analyzer.analyze(df_memory)

# Access results programmatically
for cluster in results['cluster_analyses']:
    print(f"Cluster {cluster['cluster_id']}: {cluster['owner']}")
    print(f"  Avg memory usage: {cluster['memory']['used']['mean']:.2f} MB")

# Or load from cached CSV
source = CSVSource()
df = source.fetch_jobs("jobs.csv")
results = analyzer.analyze(df)
```

## Output

The tool provides:

1. **Per-cluster analysis**: Detailed memory statistics for each cluster
2. **Overall memory histogram**: Distribution across all analyzed jobs
3. **Per-user totals**: Aggregated statistics for each user across all their clusters
4. **Over-allocation report**: Identifies clusters using <50% of requested memory

## Requirements

- Python >= 3.8
- pandas >= 1.5.0
- click >= 8.0.0
- HTCondor Python bindings (htcondor >= 24.0.0) - **optional**, required only for direct HTCondor queries

**Note:** HTCondor is only available on Linux. On macOS/Windows, you can still use the tool by working with CSV exports of job data.

## License

MIT License

## Contributing

Contributions welcome! Please open an issue or pull request.
