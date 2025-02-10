import matplotlib.pyplot as plt
import numpy as np

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

tuple_sizes = ["4 bytes", "16 bytes", "100 bytes"]
num_tuples = [672_000_000, 336_000_000, 74_800_000]

# Execution times in seconds
smb_times = [0.98, 1.37, 1.62]
flink_times = [76, 39.2, 14.5]

smb_tps = [n / t for n, t in zip(num_tuples, smb_times)]
flink_tps = [n / t for n, t in zip(num_tuples, flink_times)]

x = np.arange(len(tuple_sizes))
width = 0.35

fig, ax = plt.subplots(figsize=(8, 5))
ax.grid(axis="y", linestyle="--")
flink_bars = ax.bar(x - width/2, flink_tps, width, label="Apache Flink", color="tab:orange")
for bar in flink_bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, height,
        f'${height / (10 ** int(np.log10(height))):.1f} \\times 10^{{{int(np.log10(height))}}}$',
        ha='center', va='bottom')

smb_bars = ax.bar(x + width/2, smb_tps, width, label="SmbLockFreeBatched", color="tab:blue")
for bar in smb_bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, height,
        f'${height / (10 ** int(np.log10(height))):.1f} \\times 10^{{{int(np.log10(height))}}}$',
        ha='center', va='bottom')


# Labels and formatting
ax.set_xlabel("Tuple Size")
ax.set_ylabel("Tuples per Second")
ax.set_title("Throughput Comparison")
ax.set_xticks(x)
ax.set_xticklabels(tuple_sizes)
ax.set_ylim(0, 7.5e8)
# ax.set_yscale("log")
ax.legend()

plt.tight_layout()
plt.savefig(
    "../thesis/figures/evaluation/apache-flink-comparison.pgf",
    bbox_inches="tight",
    pad_inches=0,
)
