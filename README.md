# ART Implementation Throughput Comparison

This repository contains a Python script to generate line graphs comparing the throughput performance of different ART (Adaptive Radix Tree) implementations across varying thread counts.

## Generated Graphs

The script creates three line graphs, one for each CSV file type:

1. **`throughput_a.png`** - Throughput comparison for `a.csv` data
2. **`throughput_b.png`** - Throughput comparison for `b.csv` data  
3. **`throughput_c.png`** - Throughput comparison for `c.csv` data

Each graph shows:
- **X-axis**: Number of threads (1, 9, 18, 35, 60)
- **Y-axis**: Throughput in operations per second
- **Lines**: One line per folder/implementation
- **Scale**: Both axes use absolute values for better readability

## Folder Structure

The script analyzes data from three folders:
- `ART_LIMITED_Nodes_SCR/` - ART with limited nodes and SCR
- `ART_LIMITED_Version_num/` - ART with limited version numbers
- `ART_NO_SCR/` - ART without SCR

## Running the Script

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the script:
   ```bash
   python3 plot_throughput_graphs.py
   ```

3. View the generated graphs in the `throughput_graphs/` directory

## Data Format

Each CSV file contains columns:
- Thread Count
- Throughput (ops/s)
- Avg Latency (ns/op)
- Avg Read Latency (ns/op)
- Avg Write Latency (ns/op)

## Performance Insights

Based on the generated graphs, you can compare:
- **Scalability**: How each implementation performs as thread count increases
- **Efficiency**: Which implementation achieves higher throughput
- **Thread scaling**: Performance characteristics across different concurrency levels

The graphs use logarithmic scales to better visualize the wide range of throughput values and thread counts.
