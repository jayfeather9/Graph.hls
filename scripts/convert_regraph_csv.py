#!/usr/bin/env python3
"""
Convert ReGraph benchmark CSV to ae_plot.py baseline format.

ReGraph input CSV format (e.g. regraph_used.csv):
    variant,dataset,total_time,total_mteps

Output CSV format (for ae_plot.py --baseline-csv):
    dataset,algorithm,throughput_mteps

ReGraph app name mapping:
    bfs* -> sssp  (ReGraph 'bfs' implements SSSP/Bellman-Ford)
    pr*  -> pagerank
    cc*  -> connected_components

Usage:
    python3 scripts/convert_regraph_csv.py <input.csv> <output.csv>
    python3 scripts/convert_regraph_csv.py regraph_used.csv regraph_baseline.csv
"""
import csv
import os
import sys

VARIANT_TO_ALGO = {
    "bfs": "sssp",
    "pr": "pagerank",
    "cc": "connected_components",
}


def normalize_dataset(name):
    base = os.path.basename(str(name).strip())
    root, ext = os.path.splitext(base)
    return root if ext.lower() in {".txt", ".mtx"} else base


def variant_to_algo(variant):
    """Extract algorithm from variant name like 'bfs_270Mhz' or 'pr_270Mhz'."""
    prefix = variant.split("_")[0].lower()
    return VARIANT_TO_ALGO.get(prefix)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.csv> <output.csv>")
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]

    rows = []
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            algo = variant_to_algo(row["variant"])
            if algo is None:
                print(f"  Skipping unknown variant: {row['variant']}")
                continue
            dataset = normalize_dataset(row["dataset"])
            mteps = row["total_mteps"]
            rows.append({"dataset": dataset, "algorithm": algo, "throughput_mteps": mteps})

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "algorithm", "throughput_mteps"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Converted {len(rows)} rows: {input_path} -> {output_path}")


if __name__ == "__main__":
    main()
