import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
from pandas.core.arrays.arrow import array
from adjustText import adjust_text

parser = argparse.ArgumentParser(description="Benchmark Analysis")
parser.add_argument("--source", choices=['laptop', 'server'], required=True, help="Source of the benchmark data (laptop/server)")
parser.add_argument("--input-file", required=True, help="Path to the benchmark data file")
parser.add_argument("--output-dir", required=True, help="Directory to save the output plots")
parser.add_argument("--output-prefix", default="benchmark", help="Prefix for the output plot files")
parser.add_argument("--grouping-column", type=str, default="tuple_size-Partitions", help="Column to group data by.")

args = parser.parse_args()

columns = [
    "Benchmark", "tuple_size", "Tuples", "GB", "Partitions", "Threads",
    "time_sec", "cycles", "kcycles", "instructions", "L1_misses", "LLC_misses",
    "branch_misses", "task_clock", "scale", "IPC", "CPUs", "GHz", "tuple_size-Partitions"
]

os.makedirs(args.output_dir, exist_ok=True)

def load_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.lstrip().startswith("A-Benchmark"):
                row = line.split(",")
                row = [element.strip() for element in row]
                if len(row) > len(columns)-1:
                    row = row[:6] + row[7:]
                row.append("Tuple"+ row[1] + "-" + row[4])
                data.append(row)

    df = pd.DataFrame(data, columns=columns)
    numeric_cols = columns[1:-1]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df

def plot_individual_data(df, output_dir, output_prefix, grouping_column="tuple_size-Partitions"):
    os.makedirs(output_dir, exist_ok=True)

    for benchmark in df["Benchmark"].unique():
        df_benchmark = df[df["Benchmark"] == benchmark]

        for group_value in df_benchmark[grouping_column].unique():
            df_group = df_benchmark[df_benchmark[grouping_column] == group_value]

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(df_group["Threads"], df_group["time_sec"], marker="o", label="Time (sec)")
            texts = []
            for x, y in zip(df_group["Threads"], df_group["time_sec"]):
                text = ax.text(x, y, f"{y:.2f}", ha="center", va="bottom", fontsize=8)
                texts.append(text)

            adjust_text(texts,
                arrowprops=dict(arrowstyle='->', color='gray', lw=0.5),
                expand_text=(3, 3),
                max_move=(100,100)
            )
            ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
            ax.set_title(f"{benchmark} - {grouping_column} {group_value}")
            ax.set_xlabel("Threads")
            ax.set_ylabel("Metrics")
            ax.legend()

            output_file = f"{output_dir}/{output_prefix}_{benchmark}_{grouping_column}_{group_value}.png"
            plt.savefig(output_file)
            plt.close(fig)
            print(f"Saved plot to {output_file}")

def plot_combined_data(df, output_dir, output_prefix, grouping_column="tuple_size-Partitions"):
    os.makedirs(output_dir, exist_ok=True)
    markers = ['o', 's', 'D', '^', 'v', 'p', '*', 'h', 'x', '+']
    colors = plt.cm.tab10.colors
    for group_value in df[grouping_column].unique():
        df_group = df[df[grouping_column] == group_value]
        fig, ax = plt.subplots(figsize=(10, 6))
        texts = []
        for i, benchmark in enumerate(df["Benchmark"].unique()):
            df_benchmark = df_group[df_group["Benchmark"] == benchmark]

            if not df_benchmark.empty:
                marker = markers[i % len(markers)]
                color = colors[i % len(colors)]

                ax.plot(df_benchmark["Threads"], df_benchmark["time_sec"],
                        marker=marker, color=color, linestyle='-',
                        label=f"{benchmark} (Time sec)")
                for x, y in zip(df_benchmark["Threads"], df_benchmark["time_sec"]):
                    text = ax.text(x, y, f"{y:.2f}", ha="center", va="bottom", fontsize=8)
                    texts.append(text)

        adjust_text(texts,
                    arrowprops=dict(arrowstyle='->', color='gray', lw=0.5),
                    expand_text=(3, 3),
                    max_move=(100,100)
        )

        ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
        ax.set_title(f"Combined - {grouping_column} {group_value}")
        ax.set_xlabel("Threads")
        ax.set_ylabel("Metrics")
        ax.legend()

        output_file = f"{output_dir}/{output_prefix}_Combined_{grouping_column}_{group_value}.png"
        plt.savefig(output_file)
        plt.close(fig)
        print(f"Saved plot to {output_file}")


# python plot/benchmark_plots.py --source=laptop --input-file=../benchmark-results/laptop-2024-10-30-shuffle.txt --output-dir=plot/output --output-prefix=2024-10-30
if __name__ == "__main__":
    df = load_data(args.input_file)
    plot_individual_data(df, args.output_dir, args.output_prefix, args.grouping_column)
    plot_combined_data(df, args.output_dir, args.output_prefix, args.grouping_column)
