#!/usr/bin/env python3
"""Compare torch, raw, and USRBIO checkpoint I/O benchmark modes."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


DEFAULT_MODEL_DIR = Path("/3fs/stage/models/benchmark_compare")
RESULT_RE = re.compile(
    r"^(?P<name>\w+): (?P<gib>[0-9.]+) GiB in (?P<seconds>[0-9.]+)s = (?P<throughput>[0-9.]+) GiB/s$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare raw, torch, and USRBIO model I/O benchmark modes.")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help=f"Base directory for benchmark outputs (default: {DEFAULT_MODEL_DIR})",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=("raw", "torch", "usrbio"),
        default=("raw", "torch", "usrbio"),
        help="Formats to compare.",
    )
    parser.add_argument(
        "--mode",
        choices=("store", "load", "both"),
        default="both",
        help="Operation to benchmark.",
    )
    parser.add_argument("--num-params", type=int, default=13_000_000_000)
    parser.add_argument("--dtype", choices=("float16", "bfloat16", "float32"), default="float16")
    parser.add_argument("--shard-size-gib", type=float, default=1.0)
    parser.add_argument("--block-size-mib", type=int, default=4)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--iodepth", type=int, default=96)
    parser.add_argument("--no-shard", action="store_true")
    parser.add_argument("--touch", action="store_true")
    parser.add_argument("--mmap", action="store_true", help="Forward --mmap to torch load.")
    parser.add_argument("--keep-going", action="store_true", help="Continue if one format fails.")
    return parser.parse_args()


def benchmark_script() -> Path:
    return Path(__file__).with_name("benchmark_13b_model_io.py")


def build_command(args: argparse.Namespace, fmt: str) -> list[str]:
    cmd = [
        sys.executable,
        str(benchmark_script()),
        "--format",
        fmt,
        "--mode",
        args.mode,
        "--model-dir",
        str(args.model_dir / fmt),
        "--num-params",
        str(args.num_params),
        "--dtype",
        args.dtype,
        "--shard-size-gib",
        str(args.shard_size_gib),
        "--block-size-mib",
        str(args.block_size_mib),
        "--jobs",
        str(args.jobs),
        "--iodepth",
        str(args.iodepth),
        "--overwrite",
        "--no-progress",
    ]
    if args.no_shard:
        cmd.append("--no-shard")
    if args.touch:
        cmd.append("--touch")
    if args.mmap and fmt == "torch":
        cmd.append("--mmap")
    return cmd


def run_format(args: argparse.Namespace, fmt: str) -> list[dict[str, str]]:
    cmd = build_command(args, fmt)
    print(f"\n=== {fmt} ===")
    print(" ".join(cmd))
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(proc.stdout, end="")
    if proc.returncode != 0:
        raise RuntimeError(f"{fmt} benchmark failed with exit code {proc.returncode}")

    rows = []
    for line in proc.stdout.splitlines():
        match = RESULT_RE.match(line.strip())
        if match:
            rows.append(match.groupdict())
    return rows


def print_summary(results: list[dict[str, str]]) -> None:
    if not results:
        print("\nNo benchmark results were parsed.")
        return

    print("\nSummary")
    print(f"{'operation':<16} {'GiB':>10} {'seconds':>10} {'GiB/s':>10}")
    print("-" * 52)
    for row in results:
        print(f"{row['name']:<16} {row['gib']:>10} {row['seconds']:>10} {row['throughput']:>10}")


def main() -> int:
    args = parse_args()
    results: list[dict[str, str]] = []
    for fmt in args.formats:
        try:
            results.extend(run_format(args, fmt))
        except Exception as exc:
            if not args.keep_going:
                raise
            print(f"{fmt} failed: {exc}", file=sys.stderr)
    print_summary(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
