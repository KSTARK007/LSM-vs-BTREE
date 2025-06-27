import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Create a dictionary to store all dataframes
dataframes = {}

# List of file paths provided by the user
file_paths = [
    # 'btreeolc_results/a.csv',
    # 'btreeolc_results/c.csv',
    # 'btreeolc_results/b.csv',
    # 'fbtree_results/a.csv',
    # 'fbtree_results/c.csv',
    # 'fbtree_results/b.csv',
    # 'masstree_results/a.csv',
    # 'masstree_results/c.csv',
    # 'masstree_results/b.csv',
    # 'optiql_results/a.csv',
    # 'optiql_results/c.csv',
    # 'optiql_results/b.csv',
    # 'artolc_results/a.csv',
    # 'artolc_results/c.csv',
    # 'artolc_results/b.csv'
    'fbtree-cache-flush_results/a.csv',
    'fbtree-cache-flush_results/c.csv',
    'fbtree-cache-flush_results/b.csv',
    'artolc-cache-flush_results/a.csv',
    'artolc-cache-flush_results/c.csv',
    'artolc-cache-flush_results/b.csv'
]

# Loop through each file path, read the CSV, and store it in the dictionary
for file_path in file_paths:
    df = pd.read_csv(file_path)

    # Extract system name and workload from the file path
    system_name = file_path.split('/')[0].replace('_results', '')
    workload = file_path.split('/')[-1].replace('.csv', '')

    df['System'] = system_name
    df['Workload'] = workload

    # Rename columns to ensure consistency across all dataframes
    df.rename(columns={
        'Avg Latency (ns)': 'Avg Latency (ns/op)',
        'Avg Read Lat (ns)': 'Avg Read Latency (ns/op)',
        'Avg Write Lat (ns)': 'Avg Write Latency (ns/op)'
    }, inplace=True)

    dataframes[f'{system_name}_{workload}'] = df

# Concatenate all dataframes into a single dataframe
combined_df = pd.concat(dataframes.values(), ignore_index=True)

# Print columns to debug
print("Columns in combined_df:", combined_df.columns)

# Get unique workloads and systems
workloads = combined_df['Workload'].unique()
systems = combined_df['System'].unique()

# Define plot titles and y-axis labels
plot_configs = {
    'Throughput (ops/s)': {
        'y_label': 'Throughput (ops/s)',
        'title_suffix': 'Throughput vs. Thread Count',
        'file_suffix': 'throughput_vs_threads',
        'x_col': 'Thread Count'
    },
    'Avg Latency (ns/op)': {
        'y_label': 'Avg Latency (ns/op)', # Updated to exact column name
        'title_suffix': 'Average Latency vs. Thread Count',
        'file_suffix': 'avg_latency_vs_threads',
        'x_col': 'Thread Count'
    },
    'Avg Read Latency (ns/op)': {
        'y_label': 'Avg Read Latency (ns/op)', # Updated to exact column name
        'title_suffix': 'Average Read Latency vs. Thread Count',
        'file_suffix': 'read_latency_vs_threads',
        'x_col': 'Thread Count'
    },
    'Avg Write Latency (ns/op)': {
        'y_label': 'Avg Write Latency (ns/op)', # Updated to exact column name
        'title_suffix': 'Average Write Latency vs. Thread Count',
        'file_suffix': 'write_latency_vs_threads',
        'x_col': 'Thread Count'
    },
    'Latency vs. Throughput': {
        'y_label': 'Avg Latency (ns/op)', # Updated to exact column name
        'title_suffix': 'Latency vs. Throughput',
        'file_suffix': 'latency_vs_throughput',
        'x_col': 'Throughput (ops/s)'
    }
}

# Generate plots
for workload in workloads:
    workload_df = combined_df[combined_df['Workload'] == workload]

    for plot_type, config in plot_configs.items():
        plt.figure(figsize=(12, 7))
        sns.lineplot(
            data=workload_df,
            x=config['x_col'],
            y=config['y_label'],
            hue='System',
            marker='o'
        )

        plt.title(f'Workload {workload.upper()} - {config["title_suffix"]}')
        plt.xlabel(config['x_col'])
        plt.ylabel(config['y_label'])
        plt.grid(True, linestyle='--', alpha=0.7)
        # plt.legend(title='System')
        plt.legend(title='System', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        # Create output directory if it doesn't exist
        output_dir = os.path.join('charts', f'YCSB-{workload.upper()}_results')
        os.makedirs(output_dir, exist_ok=True)

        # Save the plot with the specified naming convention
        file_name = os.path.join(output_dir, f'{config["file_suffix"]}.png')
        plt.savefig(file_name)
        plt.close()
        print(f"Saved plot to {file_name}")
        # print(workload_df)

print(f"Generated plots for workloads: {workloads} and systems: {systems}")