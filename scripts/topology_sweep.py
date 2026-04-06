#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    dsl: Path
    out_dir: Path


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    log_file: Path | None = None,
) -> int:
    cwd_str = str(cwd) if cwd is not None else None
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    if log_file is None:
        proc = subprocess.run(cmd, cwd=cwd_str, env=merged_env)
        return proc.returncode

    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("wb") as f:
        proc = subprocess.Popen(cmd, cwd=cwd_str, env=merged_env, stdout=f, stderr=subprocess.STDOUT)
        return proc.wait()


def check_tooling() -> None:
    for tool in ["cargo", "make", "bash"]:
        if shutil.which(tool) is None:
            raise SystemExit(f"missing required tool on PATH: {tool}")


def emit_variant(variant: Variant, *, bin_name: str) -> None:
    variant.out_dir.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "cargo",
        "run",
        "--quiet",
        "--bin",
        bin_name,
        "--",
        "--emit-hls",
        str(variant.dsl),
        str(variant.out_dir),
    ]
    rc = run(cmd, cwd=REPO_ROOT)
    if rc != 0:
        raise RuntimeError(f"emit failed for {variant.name} (rc={rc})")


def build_project(project_dir: Path, *, target: str, log_file: Path, env: dict[str, str]) -> int:
    cmd = ["bash", "-lc", f"set -euo pipefail; source '{env['GRAPHYFLOW_ENV_SH']}'; make all TARGET={target}"]
    return run(cmd, cwd=project_dir, env=env, log_file=log_file)


def run_hwemu(project_dir: Path, *, dataset: Path, log_file: Path, env: dict[str, str]) -> int:
    cmd = [
        "bash",
        "-lc",
        "set -euo pipefail; "
        f"source '{env['GRAPHYFLOW_ENV_SH']}'; "
        "export GRAPHYFLOW_ALLOW_MISMATCH=0; "
        "export GRAPHYFLOW_SKIP_VERIFY=0; "
        f"./run.sh hw_emu '{dataset}'",
    ]
    return run(cmd, cwd=project_dir, env=env, log_file=log_file)


def iter_variants(selected: Iterable[str] | None) -> list[Variant]:
    dsl_dir = REPO_ROOT / "apps" / "topology_variants"
    variants = [
        ("sssp_topo_l12_b2", "12L + 2B (single little group; single big group)"),
        ("sssp_topo_l66_b2", "12L + 2B (6+6 littles; 2 bigs)"),
        ("sssp_topo_l444_b2", "12L + 2B (4+4+4 littles; 2 bigs)"),
        ("sssp_topo_l66_b11", "12L + 2B (6+6 littles; 1+1 bigs)"),
        ("sssp_topo_l3333_b11", "12L + 2B (3+3+3+3 littles; 1+1 bigs)"),
        ("sssp_topo_l55_b4", "10L + 4B (5+5 littles; 4 bigs)"),
        ("sssp_topo_l64_b4", "10L + 4B (6+4 littles; 4 bigs)"),
        ("sssp_topo_l55_b22", "10L + 4B (5+5 littles; 2+2 bigs)"),
        ("sssp_topo_l64_b22", "10L + 4B (6+4 littles; 2+2 bigs)"),
        ("sssp_topo_l334_b22", "10L + 4B (3+3+4 littles; 2+2 bigs)"),
        ("sssp_topo_l44_b33", "8L + 6B (4+4 littles; 3+3 bigs)"),
        ("sssp_topo_l8_b33", "8L + 6B (8 littles; 3+3 bigs)"),
        ("sssp_topo_l44_b222", "8L + 6B (4+4 littles; 2+2+2 bigs)"),
        ("sssp_topo_l6_b8", "6L + 8B (6 littles; 8 bigs)"),
        ("sssp_topo_l6_b44", "6L + 8B (6 littles; 4+4 bigs)"),
        ("sssp_topo_l33_b2222", "6L + 8B (3+3 littles; 2+2+2+2 bigs)"),
    ]
    names = [n for (n, _) in variants]
    if selected is not None:
        want = set(selected)
        names = [n for n in names if n in want]
        missing = want.difference(names)
        if missing:
            raise SystemExit(f"unknown variants: {sorted(missing)}")

    out_base = REPO_ROOT / "target" / "topology_sweep"
    out: list[Variant] = []
    desc_map = dict(variants)
    for name in names:
        dsl = dsl_dir / f"{name}.dsl"
        out_dir = out_base / name
        out.append(Variant(name=name, description=desc_map.get(name, ""), dsl=dsl, out_dir=out_dir))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Emit/build/test a sweep of SSSP topology variants.")
    ap.add_argument("--bin", default="refactor_Graphyflow", help="Cargo bin that supports --emit-hls")
    ap.add_argument(
        "--env-sh",
        default=os.environ.get("GRAPHYFLOW_ENV_SH", "/path/to/vitis/settings64.sh"),
        help="Environment script to source for Vitis/XRT",
    )
    ap.add_argument("--dataset", default=str(REPO_ROOT / "apps" / "test_graphs" / "sssp_small.json"))
    ap.add_argument(
        "--iters",
        type=int,
        default=0,
        help="If >0, force fixed iterations via GRAPHYFLOW_MAX_ITERS (default: run to convergence).",
    )
    ap.add_argument("--variants", nargs="*", help="Optional explicit subset of variant names")
    ap.add_argument("--hwemu-jobs", type=int, default=4, help="Parallel hw_emu jobs")
    ap.add_argument("--hw-jobs", type=int, default=2, help="Parallel hw jobs")
    ap.add_argument("--skip-hw", action="store_true", help="Only do emit + hw_emu")
    ap.add_argument("--list", action="store_true", help="List known variants and exit")
    args = ap.parse_args()

    check_tooling()

    env_sh = Path(args.env_sh)
    if not env_sh.is_file():
        raise SystemExit(f"env script not found: {env_sh}")
    dataset = Path(args.dataset)
    if not dataset.is_file():
        raise SystemExit(f"dataset not found: {dataset}")

    variants = iter_variants(args.variants)
    if args.list:
        for v in variants:
            print(f"{v.name}\t{v.description}")
        return 0

    logs_dir = REPO_ROOT / "target" / "topology_sweep_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    env = {
        "GRAPHYFLOW_ENV_SH": str(env_sh),
    }

    start = time.time()
    print(f"[sweep] emitting {len(variants)} variants into target/topology_sweep/ ...", flush=True)
    for v in variants:
        emit_variant(v, bin_name=args.bin)

    print(f"[sweep] hw_emu build+run ({args.hwemu_jobs} jobs) ...", flush=True)

    # Simple job queue without external deps.
    pending = list(variants)
    running: list[tuple[Variant, subprocess.Popen[bytes], Path, object]] = []
    passed: list[Variant] = []
    failed: list[Variant] = []

    def spawn_hwemu(v: Variant) -> subprocess.Popen[bytes]:
        log_file = logs_dir / f"{v.name}.hw_emu.log"
        iters_env = ""
        if args.iters > 0:
            iters_env = f"export GRAPHYFLOW_MAX_ITERS={args.iters}; "
        cmd = [
            "bash",
            "-lc",
            "set -euo pipefail; "
            f"source '{env['GRAPHYFLOW_ENV_SH']}'; "
            f"cd '{v.out_dir}'; "
            "make all TARGET=hw_emu; "
            "export GRAPHYFLOW_ALLOW_MISMATCH=0; "
            "export GRAPHYFLOW_SKIP_VERIFY=0; "
            + iters_env +
            f"./run.sh hw_emu '{dataset}'",
        ]
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_fh = log_file.open("wb")
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            env={**os.environ, **env},
            stdout=log_fh,
            stderr=subprocess.STDOUT,
        )
        running.append((v, proc, log_file, log_fh))
        return proc

    while pending or running:
        while pending and len(running) < args.hwemu_jobs:
            v = pending.pop(0)
            spawn_hwemu(v)

        time.sleep(5)
        still_running: list[tuple[Variant, subprocess.Popen[bytes], Path, object]] = []
        for v, proc, log_file, log_fh in running:
            rc = proc.poll()
            if rc is None:
                still_running.append((v, proc, log_file, log_fh))
                continue
            try:
                log_fh.close()
            except Exception:
                pass
            if rc == 0:
                passed.append(v)
            else:
                failed.append(v)
        running = still_running

    print(f"[sweep] hw_emu passed={len(passed)} failed={len(failed)}", flush=True)

    if args.skip_hw:
        print("[sweep] skipping hw builds (--skip-hw)", flush=True)
        print(f"[sweep] done in {time.time() - start:.1f}s", flush=True)
        if failed:
            print("[sweep] failed variants:", flush=True)
            for v in failed:
                print(f"  - {v.name}", flush=True)
        return 0 if not failed else 2

    print(f"[sweep] hw builds ({args.hw_jobs} jobs) ...", flush=True)
    pending_hw = list(passed)
    running_hw: list[tuple[Variant, subprocess.Popen[bytes], Path, object]] = []
    built_hw: list[Variant] = []
    failed_hw: list[Variant] = []

    def spawn_hw(v: Variant) -> None:
        log_file = logs_dir / f"{v.name}.hw.log"
        cmd = [
            "bash",
            "-lc",
            "set -euo pipefail; "
            f"source '{env['GRAPHYFLOW_ENV_SH']}'; "
            f"cd '{v.out_dir}'; "
            "make all TARGET=hw",
        ]
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_fh = log_file.open("wb")
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            env={**os.environ, **env},
            stdout=log_fh,
            stderr=subprocess.STDOUT,
        )
        running_hw.append((v, proc, log_file, log_fh))

    while pending_hw or running_hw:
        while pending_hw and len(running_hw) < args.hw_jobs:
            v = pending_hw.pop(0)
            spawn_hw(v)

        time.sleep(30)
        still_running_hw: list[tuple[Variant, subprocess.Popen[bytes], Path, object]] = []
        for v, proc, log_file, log_fh in running_hw:
            rc = proc.poll()
            if rc is None:
                still_running_hw.append((v, proc, log_file, log_fh))
                continue
            try:
                log_fh.close()
            except Exception:
                pass
            if rc == 0:
                built_hw.append(v)
            else:
                failed_hw.append(v)
        running_hw = still_running_hw

    print(f"[sweep] hw built={len(built_hw)} failed={len(failed_hw)}", flush=True)
    print(f"[sweep] done in {time.time() - start:.1f}s", flush=True)

    if failed or failed_hw:
        print("[sweep] failures:", flush=True)
        for v in failed:
            print(f"  - hw_emu failed: {v.name}", flush=True)
        for v in failed_hw:
            print(f"  - hw build failed: {v.name}", flush=True)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
