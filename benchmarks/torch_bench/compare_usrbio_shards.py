#!/usr/bin/env python3
"""Compare USRBIO checkpoint I/O throughput across shard counts."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


DEFAULT_MODEL_DIR = Path("/3fs/stage/models/benchmark_usrbio_shards")
GIB = 1024**3
RESULT_RE = re.compile(
    r"^(?P<name>\w+): (?P<gib>[0-9.]+) GiB in (?P<seconds>[0-9.]+)s = (?P<throughput>[0-9.]+) GiB/s$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare USRBIO model I/O with different shard counts.")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help=f"Base directory for benchmark outputs (default: {DEFAULT_MODEL_DIR})",
    )
    parser.add_argument(
        "--shards",
        nargs="+",
        type=int,
        default=(1, 2, 4, 8, 16, 32),
        help="Shard counts to test.",
    )
    parser.add_argument(
        "--mode",
        choices=("store", "load", "both"),
        default="both",
        help="Operation to benchmark.",
    )
    parser.add_argument("--num-params", type=int, default=13_000_000_000)
    parser.add_argument("--dtype", choices=("float16", "bfloat16", "float32"), default="float16")
    parser.add_argument("--block-size-mib", type=int, default=4)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--iodepth", type=int, default=96)
    parser.add_argument("--touch", action="store_true")
    parser.add_argument("--keep-going", action="store_true", help="Continue if one shard count fails.")
    return parser.parse_args()


def benchmark_script() -> Path:
    return Path(__file__).with_name("benchmark_13b_model_io.py")


def element_size(dtype: str) -> int:
    return {
        "float16": 2,
        "bfloat16": 2,
        "float32": 4,
    }[dtype]


def shard_size_gib(args: argparse.Namespace, shards: int) -> float:
    total_bytes = args.num_params * element_size(args.dtype)
    return max(1, total_bytes // shards) / GIB


def build_command(args: argparse.Namespace, shards: int) -> list[str]:
    cmd = [
        sys.executable,
        str(benchmark_script()),
        "--format",
        "usrbio",
        "--mode",
        args.mode,
        "--model-dir",
        str(args.model_dir / f"{shards}_shards"),
        "--num-params",
        str(args.num_params),
        "--dtype",
        args.dtype,
        "--shard-size-gib",
        str(shard_size_gib(args, shards)),
        "--block-size-mib",
        str(args.block_size_mib),
        "--jobs",
        str(args.jobs),
        "--iodepth",
        str(args.iodepth),
        "--overwrite",
        "--no-progress",
    ]
    if shards == 1:
        cmd.append("--no-shard")
    if args.touch:
        cmd.append("--touch")
    return cmd


def run_shard_count(args: argparse.Namespace, shards: int) -> list[dict[str, str]]:
    if shards < 1:
        raise ValueError(f"invalid shard count: {shards}")

    cmd = build_command(args, shards)
    print(f"\n=== {shards} shard{'s' if shards != 1 else ''} ===")
    print(" ".join(cmd))
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(proc.stdout, end="")
    if proc.returncode != 0:
        raise RuntimeError(f"{shards} shard benchmark failed with exit code {proc.returncode}")

    rows = []
    for line in proc.stdout.splitlines():
        match = RESULT_RE.match(line.strip())
        if match:
            row = match.groupdict()
            row["shards"] = str(shards)
            row["shard_size_gib"] = f"{shard_size_gib(args, shards):.3f}"
            rows.append(row)
    return rows


def print_summary(results: list[dict[str, str]]) -> None:
    if not results:
        print("\nNo benchmark results were parsed.")
        return

    print("\nSummary")
    print(f"{'shards':>8} {'shard GiB':>10} {'operation':<16} {'GiB':>10} {'seconds':>10} {'GiB/s':>10}")
    print("-" * 74)
    for row in results:
        print(
            f"{row['shards']:>8} {row['shard_size_gib']:>10} {row['name']:<16} "
            f"{row['gib']:>10} {row['seconds']:>10} {row['throughput']:>10}"
        )


def main() -> int:
    args = parse_args()
    results: list[dict[str, str]] = []
    for shards in args.shards:
        try:
            results.extend(run_shard_count(args, shards))
        except Exception as exc:
            if not args.keep_going:
                raise
            print(f"{shards} shard benchmark failed: {exc}", file=sys.stderr)
    print_summary(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
