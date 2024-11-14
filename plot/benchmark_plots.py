import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd
from adjustText import adjust_text

parser = argparse.ArgumentParser(description="Benchmark Analysis")
parser.add_argument("--source", choices=['laptop', 'server'], required=True,
                    help="Source of the benchmark data (laptop/server)")
parser.add_argument("--input-file", required=True, help="Path to the benchmark data file")
parser.add_argument("--output-dir", required=True, help="Directory to save the output plots")
parser.add_argument("--output-prefix", default="benchmark", help="Prefix for the output plot files")
parser.add_argument("--grouping-column", type=str, default="tuple_size-Partitions", help="Column to group data by.")

args = parser.parse_args()

columns = ["Benchmark", "tuple_size", "Tuples", "GB", "Partitions", "Threads", "time_sec", "cycles", "kcycles",
           "instructions", "L1_misses", "LLC_misses", "branch_misses", "task_clock", "scale", "IPC", "CPUs", "GHz",
           "tuple_size-Partitions"]

os.makedirs(args.output_dir, exist_ok=True)


def load_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.lstrip().startswith("A-Benchmark"):
                row = line.split(",")
                row = [element.strip() for element in row]
                if len(row) > 1:
                    row[1] = row[1].zfill(4)
                if len(row) > 4:
                    row[4] = row[4].zfill(4)
                if len(row) > len(columns) - 1:
                    row = row[:6] + row[7:]
                row.append("Tuple" + row[1] + "-" + row[4])
                row[3] = row[3].replace("GB", "")
                data.append(row)

    df = pd.DataFrame(data, columns=columns)
    numeric_cols = columns[1:-1]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df


def plot_individual_data(df, output_dir, source, output_prefix, grouping_column="tuple_size-Partitions"):
    os.makedirs(output_dir + "/" + source, exist_ok=True)

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

            adjust_text(texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5), expand_text=(3, 3),
                        max_move=(100, 100))
            tuple_generation_time = 2.3 if "server" in source else 0.8
            ax.axhline(y=tuple_generation_time, color='purple', linestyle='--', linewidth=1.5,
                       label=f"Tuple Generation ({tuple_generation_time} sec, Time sec)")

            ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
            ax.set_title(f"{benchmark} - {grouping_column} {group_value}")
            ax.set_xlabel("Threads")
            ax.set_ylabel("Metrics")
            ax.legend()

            output_file = f"{output_dir}/{source}/{output_prefix}_{benchmark}_{grouping_column}_{group_value}.png"
            plt.savefig(output_file)
            plt.close(fig)
            print(f"Saved plot to {output_file}")


def plot_combined_data(df, path, grouping_column="tuple_size-Partitions"):
    generate_combined_images(df, path, grouping_column, "Time", "time_sec", "sec", with_baseline=True)
    generate_combined_images(df, path, grouping_column, "Instructions", "instructions", "1 Mio")
    generate_combined_images(df, path, grouping_column, "L1_misses", "L1_misses", "1 Mio")
    generate_combined_images(df, path, grouping_column, "LLC_misses", "LLC_misses", "1 Mio")
    generate_combined_images(df, path, grouping_column, "Branch_misses", "branch_misses", "1 Mio")
    generate_combined_images(df, path, grouping_column, "IPC", "IPC", "")


def generate_combined_images(df, path, grouping_column, y_label, y_column, y_unit, with_baseline=False, with_y_log_scale = False):
    # Get unique group values for creating subplots
    unique_group_values = df[grouping_column].unique()
    num_groups = len(unique_group_values)
    # Create a figure with multiple subplots (one per group value)
    fig, axs = plt.subplots(ncols=2, nrows=3, figsize=(24, 18))
    if num_groups == 1:
        axs = [axs]  # Handle case when there's only one subplot
    # Loop over each group (group_value)
    for idx, group_value in enumerate(unique_group_values):
        ax = axs[int(idx / 2)][idx%2]
        if with_y_log_scale:
            ax.set_yscale("log")
        df_group = df[df[grouping_column] == group_value]
        annotations = []

        min_y, max_y = df_group[y_column].min(), df_group[y_column].max()
        if with_baseline:
            tuple_generation_time_map = [[1.41,0.48], [3.13,1.24], [3.71,1.56]]
            # Add a horizontal line for tuple generation time
            main_index = 0 if (df_group["tuple_size"] == 4).any() else (1 if(df_group["tuple_size"] == 16).any() else 2)
            sub_index = 0 if "server" in path else 1
            tuple_generation_time = tuple_generation_time_map[main_index][sub_index]
            ax.axhline(y=tuple_generation_time, color='purple', linestyle='--', linewidth=1.5,
                       label=f"Tuple Generation ({tuple_generation_time} {y_unit})")
            min_y, max_y = min(df_group[y_column].min(), tuple_generation_time), df_group[y_column].max()

        y_scale = max_y / min_y

        markers = ['o', 's', 'D', '^', 'v', 'p', '*', 'h', 'x', '+']
        colors = plt.cm.tab10.colors
        for i, benchmark in enumerate(df["Benchmark"].unique()):
            df_benchmark = df_group[df_group["Benchmark"] == benchmark]

            if not df_benchmark.empty:

                marker = markers[i % len(markers)]
                color = colors[i % len(colors)]

                # Plot the benchmark data
                ax.plot(df_benchmark["Threads"], df_benchmark[y_column], marker=marker, color=color, linestyle='-',
                        label=f"{benchmark} ")

                # Collect annotation data
                for x, y in zip(df_benchmark["Threads"], df_benchmark[y_column]):
                    annotations.append((x, y))

        annotate_with_padding(ax, annotations, y_scale)

        # Configure subplot
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
        ax.set_title(f"Combined - {grouping_column} {group_value} ({df_group["GB"].min()} GB)", fontsize=14)
        ax.set_ylabel(f"{y_label} ({y_unit})", fontsize=12)
        ax.set_xlabel("Threads", fontsize=12)
        ax.legend(fontsize=10)

        # Adjust x-axis tick labels for better readability
        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)
    # Adjust layout to fit all elements
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    # plt.subplots_adjust(hspace=0.4)
    # Save the combined plot as a single image
    output_file = f"{path}/{y_label}_Combined.svg"
    plt.savefig(output_file, format='svg', bbox_inches='tight')
    plt.close(fig)
    print(f"Saved combined plot to {output_file}")


def annotate_with_padding(ax, annotations, y_scale):
    # Check for proximity (y-values within a certain tolerance) and adjust text placement
    seen_positions = set()
    tolerance = y_scale / 4
    low_booster = min(tolerance / 7, 1)
    high_booster = max(tolerance / 7, 1)
    tolerance /= low_booster
    tolerance /= high_booster
    tolerance /= 2
    annotations.sort(key=lambda y: y[1])
    for x, y in annotations:
        offset = tolerance / 101  # Set a default offset
        adjusted_y = y

        # Check if the y-value is close to another annotation
        collapsed = True
        while collapsed:
            collapsed = False
            for other_x, other_y in seen_positions:
                if x == other_x and abs(adjusted_y - other_y) <= tolerance:
                    collapsed = True
                    adjusted_y += offset  # Adjust y to avoid overlap

        seen_positions.add((x, adjusted_y))

        # Annotate with adjusted positions
        ax.annotate(f"{y:.2f}", (x, y), textcoords="offset points", xytext=(0, 5 + (adjusted_y - y) * 2),
                                 ha='center', fontsize=9, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5))


# python plot/benchmark_plots.py --source=laptop --input-file=../benchmark-results/laptop-2024-10-30-shuffle.txt --output-dir=plot/output --output-prefix=2024-10-30
if __name__ == "__main__":
    df = load_data(args.input_file)
    path = f"{args.output_dir}/{args.source}/{args.output_prefix}"
    os.makedirs(path, exist_ok=True)
    # plot_individual_data(df, args.output_dir, args.source, args.output_prefix, args.grouping_column)
    plot_combined_data(df, path, args.grouping_column)
