import argparse
import pandas as pd
import matplotlib.pyplot as plt


parser = argparse.ArgumentParser(description="Benchmark Analysis")
parser.add_argument(
    "--source",
    choices=["laptop", "server"],
    required=True,
    help="Source of the benchmark data (laptop/server)",
)
parser.add_argument(
    "--input-file", required=True, help="Path to the benchmark data file"
)
parser.add_argument(
    "--output-dir", required=True, help="Directory to save the output plots"
)

args = parser.parse_args()


# Function to read and process the data
def read_benchmark_data(file_path):
    # Read the file and skip the header lines (comments)
    df = pd.read_csv(
        file_path,
        delimiter=",",
        comment="A",
        names=[
            "Benchmark",
            "Tuple Size",
            "Tuples",
            "GB",
            "Partitions",
            "Threads",
            "Time (s)",
            "Cycles",
            "kCycles",
            "Instructions",
            "L1 Misses",
            "LLC Misses",
            "Branch Misses",
            "Task Clock",
            "Scale",
            "IPC",
            "CPUs",
            "GHz",
        ],
        skip_blank_lines=True,
    )
    # Convert relevant columns to numeric values
    df["Tuple Size"] = pd.to_numeric(df["Tuple Size"], errors="coerce")
    df["Partitions"] = pd.to_numeric(df["Partitions"], errors="coerce")
    df["Time (s)"] = pd.to_numeric(df["Time (s)"], errors="coerce")

    # Drop any rows with missing data
    df.dropna(inplace=True)
    return df


# Function to plot time vs. number of partitions
def plot_time_vs_partitions(df, source, output_dir):
    # Filter the data for a specific benchmark (optional)
    benchmark_name = "OnDemandSingleThreadOrchestrator"
    df_filtered = df[df["Benchmark"] == benchmark_name]

    # Plot the data
    plt.figure(figsize=(20, 6))
    plt.plot(df_filtered["Partitions"], df_filtered["Time (s)"], label=benchmark_name)
    plt.xlabel("Number of Partitions")
    plt.ylabel("Time (seconds)")
    plt.title("Time vs. Number of Partitions")
    plt.grid(True)
    plt.legend()
    plt.savefig(f"{output_dir}/{source}/partitions.png")


# python plot/partition_plot.py --source=laptop --input-file=../benchmark-results/laptop-2024-11-08-partition2.txt --output-dir=plot/output
if __name__ == "__main__":
    # Read the benchmark data
    df = read_benchmark_data(args.input_file)

    # Plot the time vs. number of partitions
    plot_time_vs_partitions(df, args.source, args.output_dir)
