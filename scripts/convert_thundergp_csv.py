#!/usr/bin/env python3
"""
Convert ThunderGP benchmark CSV(s) to ae_plot.py baseline format.

ThunderGP input CSV format (from benchmark.py):
    APP_MODE,Dataset,Total_Edges,Total_E2E_Time_ms,MTEPS

Output CSV format (for ae_plot.py --baseline-csv):
    dataset,algorithm,throughput_mteps

ThunderGP app names map directly to ours:
    sssp -> sssp
    cc   -> cc
    pr   -> pr
    ar   -> ar
    wcc  -> wcc

Usage:
    # Single CSV:
    python3 scripts/convert_thundergp_csv.py <input.csv> <output.csv>

    # Multiple CSVs (one per app, merged):
    python3 scripts/convert_thundergp_csv.py <input1.csv> <input2.csv> ... <output.csv>
"""
import csv
import os
import sys


def normalize_dataset(name):
    base = os.path.basename(str(name).strip())
    root, ext = os.path.splitext(base)
    return root if ext.lower() in {".txt", ".mtx"} else base


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <input1.csv> [input2.csv ...] <output.csv>")
        sys.exit(1)

    output_path = sys.argv[-1]
    input_paths = sys.argv[1:-1]

    rows = []
    for input_path in input_paths:
        with open(input_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                algo = row["APP_MODE"].strip().lower()
                dataset = normalize_dataset(row["Dataset"])
                mteps = row.get("MTEPS", "").strip()

                if mteps in ("ERROR", "PARSE_FAIL", "SCRIPT_ERR", ""):
                    print(f"  Skipping error row: {dataset}/{algo}")
                    continue

                rows.append({
                    "dataset": dataset,
                    "algorithm": algo,
                    "throughput_mteps": mteps,
                })

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "algorithm", "throughput_mteps"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Converted {len(rows)} rows from {len(input_paths)} file(s): -> {output_path}")


if __name__ == "__main__":
    main()
