import matplotlib.pyplot as plt

plt.rcParams.update({"text.usetex": True, "pgf.texsystem": "pdflatex"})
text_keys = [
    "font.size",
    "axes.titlesize",
    "axes.labelsize",
    "xtick.labelsize",
    "ytick.labelsize",
    "legend.fontsize",
]
plt.rcParams.update(
    {
        key: plt.rcParams[key] + 4
        for key in text_keys
        if isinstance(plt.rcParams[key], (int, float))
    }
)
# Data
implementations = [
    "CmpExclPartitionRanges",
    "CmpProcessingUnits",
    "Hybrid",
    "LocalPagesAndMerge",
    "OnDemand",
    "Radix",
    "Smb*",
]
total_heap = [
    1.5980,
    1.6178,
    1.576265019365,
    6.271207471378,
    1.534321457632,
    2.56403765548,
    1.5580,
]
radix_materialization = 1.01
baseline = 1.455  # Baseline memory usage (in GiB)
baseline_values = [baseline] * len(total_heap)

# Calculate overhead (total - baseline)
overhead = [memory - baseline for memory in total_heap]

# Plot
fig, ax = plt.subplots(figsize=(12, 6))

# Plot overhead as bars
baseline_bars = ax.bar(
    implementations,
    baseline_values,
    color="tab:gray",
)
overhead_bars = ax.bar(
    implementations,
    overhead,
    bottom=baseline_values,
    color="tab:blue",
    label="Additional heap memory",
)

ax.bar(
    implementations[3],
    total_heap[3] - baseline - 0.12,
    bottom=baseline_values,
    color="tab:purple",
    label=f"Temporary Slotted Pages ({total_heap[3] - baseline - 0.12:.2f} GiB)",
)
ax.bar(
    implementations[-2],
    radix_materialization,
    bottom=baseline_values,
    color="tab:orange",
    label=f"Radix Materialization ({radix_materialization:.2f} GiB)",
)
# Add baseline line
ax.axhline(
    y=baseline,
    color="black",
    linestyle="--",
    label=f"Min. slotted pages ({baseline:.2f} GiB)",
)

# Labels and title
ax.set_ylabel("Peak Heap Usage (GiB)")
ax.set_xlabel("Implementations")
ax.set_title("Peak Heap Usage Comparison")

plt.ylim(0, 7)
# Add values on top of bars for better readability
for i, (total, overhead_value) in enumerate(zip(total_heap, overhead)):
    gap = 0.085
    if implementations[i] == "Radix":
        ax.text(
            i,
            baseline_values[i] + overhead_value + gap,
            f"{overhead_value - radix_materialization:.2f} + {radix_materialization:.2f}  GiB",
            ha="center",
        )
    elif implementations[i] == "LocalPagesAndMerge":
        ax.text(
            i,
            baseline_values[i] + overhead_value + gap,
            f"{0.12:.2f} + {total_heap[3] - baseline - 0.12:.2f} GiB",
            ha="center",
        )
    else:
        ax.text(
            i,
            baseline_values[i] + overhead_value + gap,
            f"{overhead_value:.2f} GiB",
            ha="center",
        )


# Show the plot
plt.xticks(rotation=45)
plt.tight_layout()
handles, labels = ax.get_legend_handles_labels()
# sort both labels and handles by labels
labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0]))
ax.legend(handles, labels)
plt.savefig(
    "../thesis/figures/evaluation/memory_plot-16B-P32-Th40.pgf",
    bbox_inches="tight",
    pad_inches=0,
)
