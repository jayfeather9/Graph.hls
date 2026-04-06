#!/usr/bin/env python3
"""
Universal simulation results plotter for Graphyflow.

Generates grouped bar charts from simulation CSV data, matching the style of
the paper's plot_pr_cc_sssp_speedup.py.

Usage:
    # Plot simulation times (bars only)
    python plot_sim_results.py --sim-csv results.csv --dsls sssp,pagerank,connected_components \
        --y-mode time --output sim_times.pdf

    # Plot speedup vs a baseline CSV (bars with speedup values)
    python plot_sim_results.py --sim-csv results.csv --dsls sssp,pagerank,connected_components \
        --baseline-csv swemu_times.csv --baseline-cols sssp_iter0_ms,pr_iter0_ms,cc_iter0_ms \
        --baseline-unit ms --y-mode speedup --output speedup.pdf

    # With short-name mapping
    python plot_sim_results.py --sim-csv results.csv --dsls sssp,pagerank \
        --y-mode time --short-names short_names.csv --output sim_times.pdf

Arguments:
    --sim-csv       Path to simulation results CSV (from batch_simulate.sh)
    --dsls          Comma-separated DSL names to plot (e.g. "sssp,pagerank,connected_components")
    --y-mode        "time" for absolute simulation time, "speedup" for speedup over baseline
    --output        Output PDF path (default: sim_results.pdf)
    --baseline-csv  (speedup mode) CSV with baseline timing data
    --baseline-cols (speedup mode) Comma-separated column names in baseline CSV, one per DSL
    --baseline-unit (speedup mode) Unit of baseline values: "ms" or "s" (default: ms)
    --short-names   CSV with 'dataset' and 'simple_name' columns for x-axis labels
    --extra-short   Extra short name mappings as "key1:val1,key2:val2"
    --title         Plot title (optional)
    --y-label       Y-axis label override
    --fig-width     Figure width in inches (default: 12)
    --fig-height    Figure height in inches (default: 5)
"""

import argparse
import math
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd

# ---- Visual constants (matching paper style) ----
HATCH_PATTERNS = ["///", "\\\\\\", "|||", "---", "xxx", "ooo", "...", "+++"]
HATCH_COLORS = ["#ff7f0e", "#1f77b4", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]

BAR_FACECOLOR = "white"
BAR_EDGECOLOR = "black"
BAR_WIDTH = 0.1
BAR_EDGE_WIDTH = 1.5
INTER_GROUP_SPACING = 0.4
X_AXIS_MARGIN = 0.01

FONT_FAMILY = ["Arial", "DejaVu Sans", "sans-serif"]
FONT_SIZE_GENERAL = 15
FONT_SIZE_LABEL = 18
FONT_SIZE_TICK = 15
FONT_SIZE_LEGEND = 17

AXES_LINEWIDTH = 1.5
GRID_MAJOR_STYLE = "--"
GRID_MAJOR_COLOR = "grey"
GRID_MAJOR_ALPHA = 0.5
TICK_DIRECTION = "in"
TICK_LENGTH_MAJOR = 5

LEGEND_NCOL = 3
LEGEND_BBOX = (0.5, 1.15)
LEGEND_EDGE_WIDTH = 2

OOM_TAG_TEXT = "N/A"
OOM_TAG_FONTSIZE = 12
OOM_TAG_ROTATION = 90

# Piecewise-linear mapping for speedup Y-axis (handles wide dynamic range)
Y_VALUE_BREAKPOINTS = [0, 1, 20, 60, 300, 1000]
Y_DISPLAY_BREAKPOINTS = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
Y_TICK_VALUES = [1, 20, 60, 300, 1000]
Y_TICK_POSITIONS = [0.2, 0.4, 0.6, 0.8, 1.0]


def normalize_dataset_name(name):
    base = os.path.basename(str(name).strip())
    root, ext = os.path.splitext(base)
    if ext.lower() in {".txt", ".mtx", ".json"}:
        return root
    return base


def load_short_name_map(csv_path, extra_map):
    short_map = {}
    if csv_path and os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            ds = normalize_dataset_name(str(row["dataset"]))
            short_map[ds] = str(row["simple_name"])
    if extra_map:
        for pair in extra_map.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                short_map[k.strip()] = v.strip()
    return short_map


def map_speedup_to_display(value):
    """Piecewise-linear mapping from real speedup to evenly-spaced display axis."""
    if value is None:
        return None
    xs = Y_VALUE_BREAKPOINTS
    ys = Y_DISPLAY_BREAKPOINTS
    if value <= xs[0]:
        return ys[0]
    if value >= xs[-1]:
        return ys[-1]
    for i in range(len(xs) - 1):
        x0, x1 = xs[i], xs[i + 1]
        if x0 <= value <= x1:
            if x1 == x0:
                return ys[i]
            t = (value - x0) / (x1 - x0)
            return ys[i] + t * (ys[i + 1] - ys[i])
    return ys[-1]


def compute_time_y_limits(all_values):
    """Compute Y-axis limits and ticks for time mode."""
    valid = [v for v in all_values if v is not None and v > 0]
    if not valid:
        return (0, 2), [0, 0.5, 1.0, 1.5, 2.0]
    max_v = max(valid)
    upper = max(1.0, max_v * 1.18)
    if upper <= 3:
        step = 0.5
    elif upper <= 8:
        step = 1
    elif upper <= 20:
        step = 2
    elif upper <= 50:
        step = 5
    else:
        step = 10
    top = step * math.ceil(upper / step)
    if step < 1:
        n = int(round(top / step))
        ticks = [round(i * step, 2) for i in range(n + 1)]
    else:
        top = int(top)
        ticks = list(range(0, int(top) + int(step), int(step)))
    return (0, top), ticks


def create_plot(args):
    # ---- Load simulation data ----
    sim_df = pd.read_csv(args.sim_csv)
    sim_df["dataset_norm"] = sim_df["dataset"].apply(normalize_dataset_name)

    # Dataset order: as they appear in the CSV
    dataset_order = list(dict.fromkeys(sim_df["dataset_norm"].tolist()))

    # Short names
    short_map = load_short_name_map(args.short_names, args.extra_short)
    simple_names = [short_map.get(ds, ds) for ds in dataset_order]

    # Parse DSLs
    dsls = [d.strip() for d in args.dsls.split(",")]

    # ---- Load baseline if speedup mode ----
    baseline_df = None
    baseline_cols = []
    if args.y_mode == "speedup":
        if not args.baseline_csv:
            print("error: --baseline-csv required for speedup mode", file=sys.stderr)
            sys.exit(1)
        baseline_df = pd.read_csv(args.baseline_csv)
        baseline_df["dataset_norm"] = baseline_df["dataset"].apply(normalize_dataset_name)
        baseline_df = baseline_df.set_index("dataset_norm")
        if args.baseline_cols:
            baseline_cols = [c.strip() for c in args.baseline_cols.split(",")]
        else:
            print("error: --baseline-cols required for speedup mode", file=sys.stderr)
            sys.exit(1)
        if len(baseline_cols) != len(dsls):
            print(f"error: {len(baseline_cols)} baseline columns but {len(dsls)} DSLs", file=sys.stderr)
            sys.exit(1)

    sim_df = sim_df.set_index("dataset_norm")

    # ---- Build series data ----
    series_data = []
    for i, dsl in enumerate(dsls):
        time_col = f"{dsl}_simulate_time_sec"
        values = []
        errors = []

        for ds in dataset_order:
            sim_sec = pd.to_numeric(sim_df[time_col].get(ds, pd.NA), errors="coerce")

            if args.y_mode == "time":
                if pd.isna(sim_sec) or float(sim_sec) <= 0:
                    values.append(None)
                    errors.append("error")
                else:
                    values.append(float(sim_sec))
                    errors.append(None)
            else:  # speedup
                bl_col = baseline_cols[i]
                bl_val = pd.to_numeric(baseline_df[bl_col].get(ds, pd.NA), errors="coerce")
                if pd.isna(sim_sec) or pd.isna(bl_val) or float(sim_sec) <= 0:
                    values.append(None)
                    errors.append("error")
                else:
                    bl_sec = float(bl_val) / 1000.0 if args.baseline_unit == "ms" else float(bl_val)
                    values.append(bl_sec / float(sim_sec))
                    errors.append(None)

        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        hatch_color = HATCH_COLORS[i % len(HATCH_COLORS)]
        series_data.append((dsl, values, errors, hatch, hatch_color))

    # ---- Display names for legend ----
    DSL_DISPLAY = {
        "sssp": "SSSP",
        "pagerank": "PR",
        "connected_components": "CC",
        "bfs": "BFS",
        "wcc": "WCC",
        "ar": "AR",
        "graph_coloring": "GC",
        "als": "ALS",
    }

    # ---- Plot ----
    fig_w = args.fig_width or 12
    fig_h = args.fig_height or 5

    plt.style.use("default")
    plt.rcParams.update({
        "font.size": FONT_SIZE_GENERAL,
        "axes.labelsize": FONT_SIZE_LABEL,
        "xtick.labelsize": FONT_SIZE_TICK,
        "ytick.labelsize": FONT_SIZE_TICK,
        "legend.fontsize": FONT_SIZE_LEGEND,
        "font.family": "sans-serif",
        "font.sans-serif": FONT_FAMILY,
        "hatch.linewidth": 1,
        "axes.linewidth": AXES_LINEWIDTH,
    })

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    n_cats = len(simple_names)
    n_series = len(series_data)
    x_pos = [i * INTER_GROUP_SPACING for i in range(n_cats)]
    total_group_width = n_series * BAR_WIDTH
    start_offset = -total_group_width / 2 + BAR_WIDTH / 2

    legend_handles = []
    all_plot_values = []

    for i, (dsl, values, errors, hatch, hatch_color) in enumerate(series_data):
        bar_positions = [pos + start_offset + i * BAR_WIDTH for pos in x_pos]

        if args.y_mode == "speedup":
            plot_values = [map_speedup_to_display(v) if v is not None else 0 for v in values]
        else:
            plot_values = [v if v is not None else 0 for v in values]

        all_plot_values.extend([v for v in values if v is not None])

        # Base bar (white fill)
        base_bars = ax.bar(bar_positions, plot_values, BAR_WIDTH,
                           facecolor=BAR_FACECOLOR, edgecolor="none", linewidth=0, zorder=1)
        # Hatch overlay
        ax.bar(bar_positions, plot_values, BAR_WIDTH,
               facecolor="none", edgecolor=hatch_color, linewidth=0, hatch=hatch, zorder=2)
        # Border
        ax.bar(bar_positions, plot_values, BAR_WIDTH,
               facecolor="none", edgecolor=BAR_EDGECOLOR, linewidth=BAR_EDGE_WIDTH, zorder=3)

        display_name = DSL_DISPLAY.get(dsl, dsl.upper())
        patch = mpatches.Patch(facecolor=BAR_FACECOLOR, edgecolor=hatch_color,
                               hatch=hatch, label=display_name, linewidth=LEGEND_EDGE_WIDTH)
        legend_handles.append(patch)

        # N/A tags for missing data
        for bar, err in zip(base_bars, errors):
            if err == "error":
                ax.text(bar.get_x() + bar.get_width() / 2.0, 0.03 if args.y_mode == "speedup" else 0,
                        OOM_TAG_TEXT, ha="center", va="bottom",
                        fontsize=OOM_TAG_FONTSIZE, color="black", rotation=OOM_TAG_ROTATION, zorder=4)

    # ---- Y-axis configuration ----
    if args.y_mode == "speedup":
        y_label = args.y_label or "Speedup"
        ax.set_ylim(Y_DISPLAY_BREAKPOINTS[0], Y_DISPLAY_BREAKPOINTS[-1])
        ax.set_yticks(Y_TICK_POSITIONS)
        ax.set_yticklabels([str(t) for t in Y_TICK_VALUES])
        # Reference line at speedup=1
        ax.axhline(map_speedup_to_display(1.0), color="grey", linestyle="--", linewidth=1.4, zorder=10)
    else:
        y_label = args.y_label or "Simulation Time (s)"
        ylim, yticks = compute_time_y_limits(all_plot_values)
        ax.set_ylim(ylim)
        ax.set_yticks(yticks)

    ax.set_ylabel(y_label)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(simple_names, rotation=0, ha="center")
    ax.tick_params(axis="x", which="major", length=0)
    ax.margins(x=X_AXIS_MARGIN)

    if args.title:
        ax.set_title(args.title)

    # Spine, ticks, grid
    for spine in ax.spines.values():
        spine.set_visible(True)
    ax.tick_params(axis="x", which="major", direction=TICK_DIRECTION,
                   length=TICK_LENGTH_MAJOR, top=True, bottom=False, labelbottom=True)
    ax.tick_params(axis="y", which="major", direction=TICK_DIRECTION,
                   length=TICK_LENGTH_MAJOR, right=True, left=True)
    ax.minorticks_off()
    ax.grid(False, axis="both", which="both")
    ax.set_axisbelow(True)
    ax.grid(True, axis="y", which="major", linestyle=GRID_MAJOR_STYLE,
            color=GRID_MAJOR_COLOR, alpha=GRID_MAJOR_ALPHA, zorder=0)

    # Legend
    ncol = min(len(legend_handles), LEGEND_NCOL)
    ax.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=LEGEND_BBOX,
              ncol=ncol, frameon=False, handletextpad=0.2,
              columnspacing=0.8, handlelength=1.8)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(args.output, format="pdf", bbox_inches="tight")
    print(f"saved plot to: {args.output}")

    # Print summary stats
    for dsl, values, _, _, _ in series_data:
        valid = [v for v in values if v is not None]
        if valid:
            display_name = DSL_DISPLAY.get(dsl, dsl)
            if args.y_mode == "speedup":
                avg = sum(valid) / len(valid)
                print(f"  {display_name}: avg speedup x{avg:.2f} ({len(valid)}/{len(values)} datasets)")
            else:
                avg = sum(valid) / len(valid)
                print(f"  {display_name}: avg time {avg:.3f}s ({len(valid)}/{len(values)} datasets)")


def main():
    parser = argparse.ArgumentParser(description="Plot Graphyflow simulation results")
    parser.add_argument("--sim-csv", required=True, help="Simulation results CSV")
    parser.add_argument("--dsls", required=True, help="Comma-separated DSL names")
    parser.add_argument("--y-mode", choices=["time", "speedup"], default="time",
                        help="Y-axis mode: absolute time or speedup over baseline")
    parser.add_argument("--output", default="sim_results.pdf", help="Output PDF path")
    parser.add_argument("--baseline-csv", help="Baseline timing CSV (for speedup mode)")
    parser.add_argument("--baseline-cols", help="Comma-separated baseline column names")
    parser.add_argument("--baseline-unit", choices=["ms", "s"], default="ms",
                        help="Baseline timing unit")
    parser.add_argument("--short-names", help="CSV with 'dataset','simple_name' columns")
    parser.add_argument("--extra-short", help="Extra short names as 'key1:val1,key2:val2'")
    parser.add_argument("--title", help="Plot title")
    parser.add_argument("--y-label", help="Y-axis label override")
    parser.add_argument("--fig-width", type=float, help="Figure width (inches)")
    parser.add_argument("--fig-height", type=float, help="Figure height (inches)")

    args = parser.parse_args()
    create_plot(args)


if __name__ == "__main__":
    main()
