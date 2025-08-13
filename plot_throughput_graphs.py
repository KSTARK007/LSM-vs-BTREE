#!/usr/bin/env python3
"""
Script to generate line graphs comparing throughput across different ART implementation folders.
Creates 3 graphs: one for each CSV file type (a.csv, b.csv, c.csv)
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

def load_csv_data(folder_path, csv_filename):
    """Load CSV data from a specific folder and file"""
    file_path = os.path.join(folder_path, csv_filename)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return df
    return None

def create_throughput_graph(csv_filename, folder_data, output_filename):
    """Create a throughput vs thread count graph for a specific CSV file"""
    plt.figure(figsize=(10, 6))
    
    # Plot each folder's data as a separate line
    for folder_name, data in folder_data.items():
        if data is not None:
            plt.plot(data['Thread Count'], data['Throughput (ops/s)'], 
                    marker='o', linewidth=2, markersize=6, label=folder_name)
    
    plt.xlabel('Number of Threads', fontsize=12)
    plt.ylabel('Throughput (ops/s)', fontsize=12)
    plt.title(f'Throughput Comparison - {csv_filename}', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)
    # plt.xscale('log')  # Removed log scale for thread count - now using absolute values
    # plt.yscale('log')  # Removed log scale for throughput - now using absolute values
    
    # Set x-axis limits and ticks for uniform spacing
    plt.xlim(0, 65)
    plt.xticks([1, 9, 18, 35, 60])
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated graph: {output_filename}")

def main():
    # Define the folders to analyze
    folders = [
        'ART_LIMITED_Nodes_SCR',
        'ART_LIMITED_Version_num', 
        'ART_NO_SCR'
    ]
    
    # Define the CSV files to process
    csv_files = ['a.csv', 'b.csv', 'c.csv']
    
    # Create output directory for graphs
    output_dir = 'throughput_graphs'
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each CSV file
    for csv_file in csv_files:
        print(f"\nProcessing {csv_file}...")
        
        # Load data from each folder for this CSV file
        folder_data = {}
        for folder in folders:
            data = load_csv_data(folder, csv_file)
            if data is not None:
                folder_data[folder] = data
                print(f"  Loaded data from {folder}: {len(data)} rows")
            else:
                print(f"  Warning: Could not load data from {folder}/{csv_file}")
        
        if folder_data:
            # Create the graph
            output_filename = os.path.join(output_dir, f'throughput_{csv_file.replace(".csv", "")}.png')
            create_throughput_graph(csv_file, folder_data, output_filename)
        else:
            print(f"  Error: No data could be loaded for {csv_file}")
    
    print(f"\nAll graphs have been generated in the '{output_dir}' directory!")

if __name__ == "__main__":
    main()
