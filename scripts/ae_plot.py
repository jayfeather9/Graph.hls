#!/usr/bin/env python3
"""
ae_plot.py — Consolidated AE figure plotter for Graph.hls.

Usage:
    python scripts/ae_plot.py --fig7 --csv target/ae/fig7/results.csv [--baseline-csv regraph.csv] -o fig7.pdf
    python scripts/ae_plot.py --fig8 --csv target/ae/fig8/results.csv [--baseline-csv thundergp.csv] -o fig8.pdf
    python scripts/ae_plot.py --fig9 --csv target/ae/fig9/results.csv --baseline-csv regraph_sssp.csv -o fig9.pdf
    python scripts/ae_plot.py --fig10 --sim-csv sim_results.csv --csim-csv csim_results.csv -o fig10.pdf

CSV format for fig7/fig8/fig9 results:
    dataset,algorithm,kernel_time_ms,throughput_mteps,status

CSV format for baseline (regraph/thundergp):
    dataset,algorithm,throughput_mteps

ReGraph app name mapping (use our algorithm names in the baseline CSV):
    regraph bfs -> sssp           (ReGraph 'bfs' implements SSSP/Bellman-Ford)
    regraph pr  -> pagerank
    regraph cc  -> connected_components
"""

import argparse
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np

# ── Dataset display names ─────────────────────────────────────────────────────
DATASET_ORDER = [
    "rmat-19-32", "rmat-21-32", "rmat-24-16",
    "graph500-scale23-ef16_adj",
    "amazon-2008", "ca-hollywood-2009",
    "dbpedia-link", "soc-flickr-und",
    "soc-LiveJournal1", "soc-orkut-dir",
    "web-baidu-baike", "web-Google",
    "web-hudong", "wiki-topcats",
]

SHORT_NAMES = {
    "rmat-19-32": "R19", "rmat-21-32": "R21", "rmat-24-16": "R24",
    "graph500-scale23-ef16_adj": "G23",
    "amazon-2008": "AM", "ca-hollywood-2009": "HW",
    "dbpedia-link": "DB", "soc-flickr-und": "FU",
    "soc-LiveJournal1": "LJ", "soc-orkut-dir": "OR",
    "web-baidu-baike": "BB", "web-Google": "GG",
    "web-hudong": "HD", "wiki-topcats": "TC",
}

# ── Common styling ────────────────────────────────────────────────────────────
FONT_FAMILY = ["Arial", "DejaVu Sans", "sans-serif"]
FONT_SIZE = 15
BAR_FACECOLOR = "white"
BAR_EDGECOLOR = "black"
BAR_EDGE_WIDTH = 1.5
AXES_LINEWIDTH = 1.5
REFERENCE_LINE_Y = 1.0


def normalize_name(name):
    base = os.path.basename(str(name).strip())
    root, ext = os.path.splitext(base)
    return root if ext.lower() in {".txt", ".mtx"} else base


def setup_style():
    plt.style.use("default")
    plt.rcParams.update({
        "font.size": FONT_SIZE,
        "axes.labelsize": 18,
        "xtick.labelsize": FONT_SIZE,
        "ytick.labelsize": FONT_SIZE,
        "legend.fontsize": 14,
        "font.family": "sans-serif",
        "font.sans-serif": FONT_FAMILY,
        "hatch.linewidth": 1,
        "axes.linewidth": AXES_LINEWIDTH,
    })


def add_reference_line(ax, y=1.0):
    ax.axhline(y, color="grey", linestyle="--", linewidth=1.4, zorder=10)


def style_axes(ax):
    for spine in ax.spines.values():
        spine.set_visible(True)
    ax.tick_params(axis="x", direction="in", length=5, top=True, bottom=False,
                   labeltop=False, labelbottom=True)
    ax.tick_params(axis="y", direction="in", length=5, right=True, left=True,
                   labelright=False, labelleft=True)
    ax.minorticks_off()
    ax.grid(True, axis="y", which="major", linestyle="--", color="grey", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)


# =============================================================================
# Figure 7: Speedup over ReGraph (PR, CC, SSSP) — grouped bars per algorithm
# =============================================================================
def plot_fig7(csv_path, baseline_csv, output):
    df = pd.read_csv(csv_path)
    df["dataset"] = df["dataset"].apply(normalize_name)

    algos = ["pagerank", "connected_components", "sssp"]
    algo_labels = {"pagerank": "PR", "connected_components": "CC", "sssp": "SSSP"}

    baseline = {}
    if baseline_csv and os.path.exists(baseline_csv):
        bdf = pd.read_csv(baseline_csv)
        bdf["dataset"] = bdf["dataset"].apply(normalize_name)
        for _, row in bdf.iterrows():
            baseline[(row["dataset"], row["algorithm"])] = float(row["throughput_mteps"])

    datasets = [d for d in DATASET_ORDER if d in df["dataset"].values]
    short = [SHORT_NAMES.get(d, d) for d in datasets]

    setup_style()
    fig, axes = plt.subplots(1, 3, figsize=(18, 4.5), sharey=True)

    for ax_idx, algo in enumerate(algos):
        ax = axes[ax_idx]
        adf = df[df["algorithm"] == algo]
        speedups = []
        for d in datasets:
            row = adf[adf["dataset"] == d]
            sg_tput = float(row["throughput_mteps"].iloc[0]) if len(row) > 0 and pd.notna(row["throughput_mteps"].iloc[0]) else None
            base_tput = baseline.get((d, algo))
            if sg_tput and base_tput and base_tput > 0:
                speedups.append(sg_tput / base_tput)
            else:
                speedups.append(None)

        x = np.arange(len(datasets))
        vals = [v if v else 0 for v in speedups]
        bars = ax.bar(x, vals, 0.6, facecolor="#1f77b4", edgecolor=BAR_EDGECOLOR,
                      linewidth=BAR_EDGE_WIDTH, alpha=0.8, zorder=2)
        add_reference_line(ax)
        style_axes(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(short, rotation=0, ha="center", fontsize=11)
        ax.set_title(algo_labels[algo], fontsize=16, fontweight="bold")
        ax.set_ylabel("Speedup" if ax_idx == 0 else "")
        ax.tick_params(axis="x", length=0)

        valid = [v for v in speedups if v]
        if valid:
            ax.set_ylim(0, max(max(valid) * 1.15, 2))
            avg = sum(valid) / len(valid)
            ax.set_title(f"{algo_labels[algo]} (avg {avg:.1f}x)", fontsize=16, fontweight="bold")

    fig.tight_layout()
    plt.savefig(output, format="pdf", bbox_inches="tight")
    print(f"Fig 7 saved to {output}")


# =============================================================================
# Figure 8: Speedup over ThunderGP (PR, WSSSP, CC, AR, WCC)
# =============================================================================
def plot_fig8(csv_path, baseline_csv, output):
    df = pd.read_csv(csv_path)
    df["dataset"] = df["dataset"].apply(normalize_name)

    algos = ["pr", "sssp", "cc", "ar", "wcc"]
    algo_labels = {"pr": "PR", "sssp": "W-SSSP", "cc": "CC", "ar": "AR", "wcc": "WCC"}

    baseline = {}
    if baseline_csv and os.path.exists(baseline_csv):
        bdf = pd.read_csv(baseline_csv)
        bdf["dataset"] = bdf["dataset"].apply(normalize_name)
        for _, row in bdf.iterrows():
            baseline[(row["dataset"], row["algorithm"])] = float(row["throughput_mteps"])

    datasets = [d for d in DATASET_ORDER if d in df["dataset"].values]
    short = [SHORT_NAMES.get(d, d) for d in datasets]

    setup_style()
    fig, axes = plt.subplots(1, 5, figsize=(24, 4.5), sharey=True)

    for ax_idx, algo in enumerate(algos):
        ax = axes[ax_idx]
        adf = df[df["algorithm"] == algo]
        speedups = []
        for d in datasets:
            row = adf[adf["dataset"] == d]
            sg_tput = float(row["throughput_mteps"].iloc[0]) if len(row) > 0 and pd.notna(row["throughput_mteps"].iloc[0]) else None
            base_tput = baseline.get((d, algo))
            if sg_tput and base_tput and base_tput > 0:
                speedups.append(sg_tput / base_tput)
            else:
                speedups.append(None)

        x = np.arange(len(datasets))
        vals = [v if v else 0 for v in speedups]
        bars = ax.bar(x, vals, 0.6, facecolor="#ff7f0e", edgecolor=BAR_EDGECOLOR,
                      linewidth=BAR_EDGE_WIDTH, alpha=0.8, zorder=2)
        add_reference_line(ax)
        style_axes(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(short, rotation=0, ha="center", fontsize=10)
        ax.set_title(algo_labels[algo], fontsize=16, fontweight="bold")
        ax.set_ylabel("Speedup" if ax_idx == 0 else "")
        ax.tick_params(axis="x", length=0)

        valid = [v for v in speedups if v]
        if valid:
            ax.set_ylim(0, max(max(valid) * 1.15, 2))
            avg = sum(valid) / len(valid)
            ax.set_title(f"{algo_labels[algo]} (avg {avg:.1f}x)", fontsize=16, fontweight="bold")

    fig.tight_layout()
    plt.savefig(output, format="pdf", bbox_inches="tight")
    print(f"Fig 8 saved to {output}")


# =============================================================================
# Figure 9: Ablation 5-bar chart (Naive, L1, L1+L2, L1+L3, L1+L2+L3)
# =============================================================================
def plot_fig9(csv_path, baseline_csv, output):
    df = pd.read_csv(csv_path)
    df["dataset"] = df["dataset"].apply(normalize_name)

    # Load ReGraph baseline for SSSP
    baseline = {}
    if baseline_csv and os.path.exists(baseline_csv):
        bdf = pd.read_csv(baseline_csv)
        bdf["dataset"] = bdf["dataset"].apply(normalize_name)
        for _, row in bdf.iterrows():
            ds = row["dataset"]
            val = float(row["throughput_mteps"]) if pd.notna(row.get("throughput_mteps")) else None
            if val and val > 0:
                baseline[ds] = val

    configs = ["naive", "l1", "l1l2", "l1l3", "l1l2l3"]
    config_labels = ["Naive", "L1", "L1+L2", "L1+L3", "L1+L2+L3"]
    hatches = ["---", "///", "\\\\\\", "|||", "xxx"]
    hatch_colors = ["#bcbd22", "#ff7f0e", "#1f77b4", "#2ca02c", "#2f6b7c"]

    datasets = [d for d in DATASET_ORDER if d in df["dataset"].values]
    short = [SHORT_NAMES.get(d, d) for d in datasets]

    setup_style()
    n_ds = len(datasets)
    n_series = len(configs)
    bar_width = 0.1
    spacing = 0.6
    x_pos = [i * spacing for i in range(n_ds)]
    total_w = n_series * bar_width
    start_off = -total_w / 2 + bar_width / 2

    fig, ax = plt.subplots(figsize=(12, 5))
    legend_handles = []

    for si, (config, label, hatch, hcolor) in enumerate(zip(configs, config_labels, hatches, hatch_colors)):
        cdf = df[df["algorithm"] == config]
        speedups = []
        errors = []
        for d in datasets:
            row = cdf[cdf["dataset"] == d]
            sg_tput = float(row["throughput_mteps"].iloc[0]) if len(row) > 0 and pd.notna(row["throughput_mteps"].iloc[0]) else None
            base = baseline.get(d)
            if sg_tput and base and base > 0:
                speedups.append(sg_tput / base)
                errors.append(None)
            else:
                speedups.append(None)
                errors.append("error")

        bar_pos = [p + start_off + si * bar_width for p in x_pos]
        vals = [v if v else 0 for v in speedups]

        ax.bar(bar_pos, vals, bar_width, facecolor=BAR_FACECOLOR, edgecolor="none", linewidth=0, zorder=1)
        ax.bar(bar_pos, vals, bar_width, facecolor="none", edgecolor=hcolor, linewidth=0, hatch=hatch, zorder=2)
        ax.bar(bar_pos, vals, bar_width, facecolor="none", edgecolor=BAR_EDGECOLOR, linewidth=BAR_EDGE_WIDTH, zorder=3)

        patch = mpatches.Patch(facecolor=BAR_FACECOLOR, edgecolor=hcolor, hatch=hatch, label=label, linewidth=2)
        legend_handles.append(patch)

        for bar, err in zip(ax.patches[-n_ds:], errors):
            if err == "error":
                ax.text(bar.get_x() + bar.get_width() / 2.0, 0.06, "OoM",
                        ha="center", va="bottom", fontsize=10, rotation=90, zorder=4)

    add_reference_line(ax)
    style_axes(ax)
    ax.set_ylabel("Speedup")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(short, rotation=0, ha="center")
    ax.tick_params(axis="x", length=0)
    ax.margins(x=0.01)

    # Auto y-limits
    all_vals = [v for v in df["throughput_mteps"].dropna()]
    ax.set_ylim(0, 7)
    ax.set_yticks(range(8))

    ax.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.5, 1.15),
              ncol=5, frameon=False, handletextpad=0.2, columnspacing=0.8, handlelength=1.8)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(output, format="pdf", bbox_inches="tight")

    # Print averages
    print("Average speedup over ReGraph:")
    for config, label in zip(configs, config_labels):
        cdf = df[df["algorithm"] == config]
        valid = []
        for d in datasets:
            row = cdf[cdf["dataset"] == d]
            sg_tput = float(row["throughput_mteps"].iloc[0]) if len(row) > 0 and pd.notna(row["throughput_mteps"].iloc[0]) else None
            base = baseline.get(d)
            if sg_tput and base and base > 0:
                valid.append(sg_tput / base)
        if valid:
            print(f"  {label}: {sum(valid)/len(valid):.2f}x")
    print(f"Fig 9 saved to {output}")


# =============================================================================
# Figure 10: SG-Scope simulation speedup over C-Sim
# =============================================================================
def plot_fig10(sim_csv, csim_csv, output):
    sim_df = pd.read_csv(sim_csv)
    sim_df["dataset"] = sim_df["dataset"].apply(normalize_name)
    csim_df = pd.read_csv(csim_csv)
    csim_df["dataset"] = csim_df["dataset"].apply(normalize_name)

    algos = ["pagerank", "connected_components", "sssp"]
    algo_labels = {"pagerank": "PR", "connected_components": "CC", "sssp": "SSSP"}

    datasets = [d for d in DATASET_ORDER if d in sim_df["dataset"].values]
    short = [SHORT_NAMES.get(d, d) for d in datasets]

    setup_style()
    fig, axes = plt.subplots(1, 3, figsize=(18, 4.5), sharey=True)

    for ax_idx, algo in enumerate(algos):
        ax = axes[ax_idx]
        speedups = []
        for d in datasets:
            sim_row = sim_df[(sim_df["dataset"] == d) & (sim_df["algorithm"] == algo)]
            csim_row = csim_df[(csim_df["dataset"] == d) & (csim_df["algorithm"] == algo)]
            sim_time = float(sim_row["time_sec"].iloc[0]) if len(sim_row) > 0 else None
            csim_time = float(csim_row["time_sec"].iloc[0]) if len(csim_row) > 0 else None
            if sim_time and csim_time and sim_time > 0:
                speedups.append(csim_time / sim_time)
            else:
                speedups.append(None)

        x = np.arange(len(datasets))
        vals = [v if v else 0 for v in speedups]
        ax.bar(x, vals, 0.6, facecolor="#2ca02c", edgecolor=BAR_EDGECOLOR,
               linewidth=BAR_EDGE_WIDTH, alpha=0.8, zorder=2)
        style_axes(ax)
        ax.set_xticks(x)
        ax.set_xticklabels(short, rotation=0, ha="center", fontsize=11)
        ax.set_ylabel("Speedup over C-Sim" if ax_idx == 0 else "")
        ax.tick_params(axis="x", length=0)

        valid = [v for v in speedups if v]
        if valid:
            avg = sum(valid) / len(valid)
            ax.set_title(f"{algo_labels[algo]} (avg {avg:.1f}x)", fontsize=16, fontweight="bold")

    fig.tight_layout()
    plt.savefig(output, format="pdf", bbox_inches="tight")
    print(f"Fig 10 saved to {output}")


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="AE figure plotter for Graph.hls")
    parser.add_argument("--fig7", action="store_true")
    parser.add_argument("--fig8", action="store_true")
    parser.add_argument("--fig9", action="store_true")
    parser.add_argument("--fig10", action="store_true")
    parser.add_argument("--csv", help="Main results CSV")
    parser.add_argument("--baseline-csv", help="Baseline CSV (ReGraph or ThunderGP)")
    parser.add_argument("--sim-csv", help="SG-Scope simulation CSV (fig10)")
    parser.add_argument("--csim-csv", help="C-Sim baseline CSV (fig10)")
    parser.add_argument("-o", "--output", default="figure.pdf", help="Output PDF path")
    args = parser.parse_args()

    if args.fig7:
        plot_fig7(args.csv, args.baseline_csv, args.output)
    elif args.fig8:
        plot_fig8(args.csv, args.baseline_csv, args.output)
    elif args.fig9:
        plot_fig9(args.csv, args.baseline_csv, args.output)
    elif args.fig10:
        plot_fig10(args.sim_csv, args.csim_csv, args.output)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
