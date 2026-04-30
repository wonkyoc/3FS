#!/usr/bin/env python3
"""Benchmark storing and loading a synthetic 13B-parameter model checkpoint.

The benchmark writes sharded tensor checkpoint files to /3fs/stage/models by
default. It uses synthetic tensors so it does not require downloading a real
13B model, while still measuring the load/store path for a model-sized
checkpoint.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD_PY = REPO_ROOT / "build/src/lib/py"
sys.path.insert(0, str(REPO_ROOT))
if BUILD_PY.exists():
    sys.path.insert(0, str(BUILD_PY))

# Example import for the 3FS USRBIO Python wrapper.
# hf3fs_fuse.io wraps the compiled hf3fs_py_usrbio pybind module.
USRBIO_IMPORT_ERROR = None
try:
    import hf3fs_fuse.io as h3io
except ImportError as exc:
    h3io = None
    USRBIO_IMPORT_ERROR = exc


DEFAULT_MODEL_DIR = Path("/3fs/stage/models/benchmark_13b_fp16")
DEFAULT_NUM_PARAMS = 13_000_000_000
GIB = 1024**3
MIB = 1024**2


class ProgressBar:
    def __init__(self, label: str, total: int, enabled: bool = True) -> None:
        self.label = label
        self.total = max(1, total)
        self.enabled = enabled
        self.done = 0
        self.start = 0.0
        self.last_render = 0.0
        self.lock = threading.Lock()

    def __enter__(self) -> "ProgressBar":
        self.start = time.perf_counter()
        self.render(force=True)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is None:
            self.done = self.total
        self.render(force=True)
        if self.enabled:
            sys.stderr.write("\n")
            sys.stderr.flush()

    def update(self, count: int) -> None:
        if count <= 0:
            return
        with self.lock:
            self.done = min(self.total, self.done + count)
            self.render()

    def render(self, force: bool = False) -> None:
        if not self.enabled:
            return
        now = time.perf_counter()
        if not force and now - self.last_render < 0.1:
            return
        self.last_render = now
        width = 28
        frac = min(1.0, self.done / self.total)
        filled = int(width * frac)
        elapsed = max(1e-9, now - self.start) if self.start else 0.0
        rate = self.done / elapsed if elapsed else 0.0
        sys.stderr.write(
            f"\r{self.label}: [{'#' * filled}{'.' * (width - filled)}] "
            f"{frac * 100:6.2f}% {format_bytes(self.done)}/{format_bytes(self.total)} "
            f"{format_bytes(rate)}/s"
        )
        sys.stderr.flush()


def format_bytes(size: float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024


def import_torch() -> Any:
    try:
        import torch
    except ImportError as exc:
        raise SystemExit("This benchmark requires PyTorch. Install torch and retry.") from exc
    return torch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark load/store throughput for a synthetic 13B model checkpoint."
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help=f"Directory for benchmark checkpoint shards (default: {DEFAULT_MODEL_DIR})",
    )
    parser.add_argument(
        "--mode",
        choices=("store", "load", "both"),
        default="both",
        help="Benchmark operation to run.",
    )
    parser.add_argument(
        "--format",
        choices=("torch", "raw", "usrbio"),
        default="torch",
        help="'raw' uses blocking file IO; 'usrbio' uses the 3FS async USRBIO API.",
    )
    parser.add_argument(
        "--num-params",
        type=int,
        default=DEFAULT_NUM_PARAMS,
        help=f"Number of synthetic model parameters (default: {DEFAULT_NUM_PARAMS}).",
    )
    parser.add_argument(
        "--dtype",
        choices=("float16", "bfloat16", "float32"),
        default="float16",
        help="Tensor dtype used to size the synthetic checkpoint.",
    )
    parser.add_argument(
        "--shard-size-gib",
        type=float,
        default=1.0,
        help="Target uncompressed tensor bytes per shard in GiB.",
    )
    parser.add_argument(
        "--no-shard",
        action="store_true",
        help="Store the checkpoint as one file instead of sharding by --shard-size-gib.",
    )
    parser.add_argument(
        "--block-size-mib",
        type=int,
        default=4,
        help="Block size used by --format raw.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Number of checkpoint shards to store/load concurrently.",
    )
    parser.add_argument(
        "--iodepth",
        type=int,
        default=96,
        help="Outstanding 4 MiB operations per USRBIO worker.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove an existing benchmark checkpoint before storing.",
    )
    parser.add_argument(
        "--touch",
        action="store_true",
        help="Touch loaded tensor data to make each load observable to Python.",
    )
    parser.add_argument(
        "--mmap",
        action="store_true",
        help="Use torch.load(..., mmap=True) for torch-format loads when supported.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress output.",
    )
    return parser.parse_args()


def dtype_from_name(torch: Any, name: str) -> Any:
    return {
        "float16": torch.float16,
        "bfloat16": torch.bfloat16,
        "float32": torch.float32,
    }[name]


def element_size_from_dtype(dtype_name: str) -> int:
    return {
        "float16": 2,
        "bfloat16": 2,
        "float32": 4,
    }[dtype_name]


def import_usrbio() -> Any:
    if h3io is None:
        detail = f" Import failed with: {USRBIO_IMPORT_ERROR}" if USRBIO_IMPORT_ERROR else ""
        raise SystemExit(
            "USRBIO mode requires hf3fs_py_usrbio. Build target hf3fs_py_usrbio "
            f"or add build/src/lib/py to PYTHONPATH.{detail}"
        )
    return h3io


def manifest_path(model_dir: Path) -> Path:
    return model_dir / "manifest.json"


def write_manifest(
    model_dir: Path,
    dtype_name: str,
    num_params: int,
    shard_size_bytes: int,
    shards: list[dict[str, Any]],
    elapsed_s: float,
) -> None:
    payload = {
        "description": "Synthetic 13B model IO benchmark checkpoint",
        "format": "torch",
        "dtype": dtype_name,
        "num_params": num_params,
        "total_tensor_bytes": sum(shard["tensor_bytes"] for shard in shards),
        "shard_size_bytes": shard_size_bytes,
        "store_elapsed_s": elapsed_s,
        "shards": shards,
    }
    manifest_path(model_dir).write_text(json.dumps(payload, indent=2) + "\n")


def write_raw_manifest(
    model_dir: Path,
    dtype_name: str,
    num_params: int,
    shard_size_bytes: int,
    shards: list[dict[str, Any]],
    elapsed_s: float,
    fmt: str = "raw",
) -> None:
    payload = {
        "description": f"Synthetic 13B model {fmt} IO benchmark",
        "format": fmt,
        "dtype": dtype_name,
        "num_params": num_params,
        "total_tensor_bytes": sum(shard["tensor_bytes"] for shard in shards),
        "shard_size_bytes": shard_size_bytes,
        "store_elapsed_s": elapsed_s,
        "shards": shards,
    }
    manifest_path(model_dir).write_text(json.dumps(payload, indent=2) + "\n")


def read_manifest(model_dir: Path) -> dict[str, Any]:
    path = manifest_path(model_dir)
    if not path.exists():
        raise SystemExit(f"Missing manifest: {path}. Run with --mode store or --mode both first.")
    return json.loads(path.read_text())


def prepare_store_dir(model_dir: Path, overwrite: bool) -> None:
    if model_dir.exists() and overwrite:
        shutil.rmtree(model_dir)
    if model_dir.exists() and any(model_dir.iterdir()):
        raise SystemExit(f"{model_dir} is not empty. Use --overwrite to replace it.")
    model_dir.mkdir(parents=True, exist_ok=True)


def shard_specs(
    num_params: int,
    element_size: int,
    shard_size_bytes: int,
    suffix: str,
    no_shard: bool = False,
) -> list[dict[str, Any]]:
    if no_shard:
        return [
            {
                "file": f"model.{suffix}",
                "params": num_params,
                "tensor_bytes": num_params * element_size,
            }
        ]

    params_per_shard = max(1, shard_size_bytes // element_size)
    num_shards = math.ceil(num_params / params_per_shard)
    specs: list[dict[str, Any]] = []
    written_params = 0
    for shard_idx in range(num_shards):
        shard_params = min(params_per_shard, num_params - written_params)
        tensor_bytes = shard_params * element_size
        specs.append(
            {
                "file": f"model-{shard_idx:05d}-of-{num_shards:05d}.{suffix}",
                "params": shard_params,
                "tensor_bytes": tensor_bytes,
            }
        )
        written_params += shard_params
    return specs


def benchmark_store(args: argparse.Namespace, torch: Any) -> dict[str, float]:
    dtype = dtype_from_name(torch, args.dtype)
    element_size = torch.empty((), dtype=dtype).element_size()
    total_bytes = args.num_params * element_size
    shard_size_bytes = max(element_size, int(args.shard_size_gib * GIB))
    shards = shard_specs(args.num_params, element_size, shard_size_bytes, "pt", args.no_shard)

    prepare_store_dir(args.model_dir, args.overwrite)

    def store_shard(shard: dict[str, Any]) -> dict[str, Any]:
        shard_params = int(shard["params"])
        tensor = torch.empty(shard_params, dtype=dtype)
        shard_path = args.model_dir / shard["file"]
        torch.save({"weight": tensor}, shard_path)
        del tensor
        return {**shard, "file_bytes": shard_path.stat().st_size}

    start = time.perf_counter()
    with ProgressBar("torch_store", total_bytes, not args.no_progress) as progress:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = [executor.submit(store_shard, shard) for shard in shards]
            shards = []
            for future in as_completed(futures):
                shard = future.result()
                progress.update(int(shard["tensor_bytes"]))
                shards.append(shard)
    shards.sort(key=lambda shard: shard["file"])

    elapsed_s = time.perf_counter() - start
    write_manifest(args.model_dir, args.dtype, args.num_params, shard_size_bytes, shards, elapsed_s)
    return {"elapsed_s": elapsed_s, "bytes": total_bytes}


def benchmark_load(args: argparse.Namespace, torch: Any) -> dict[str, float]:
    manifest = read_manifest(args.model_dir)
    if manifest.get("format", "torch") != "torch":
        raise SystemExit(f"{args.model_dir} contains a {manifest.get('format')} checkpoint, not torch.")

    def load_shard(shard: dict[str, Any]) -> tuple[int, float]:
        shard_path = args.model_dir / shard["file"]
        load_kwargs = {"map_location": "cpu", "weights_only": True}
        if args.mmap:
            load_kwargs["mmap"] = True
        try:
            checkpoint = torch.load(shard_path, **load_kwargs)
        except TypeError:
            checkpoint = torch.load(shard_path, map_location="cpu")
        tensor = checkpoint["weight"]
        checksum = 0.0
        if args.touch and tensor.numel():
            checksum += float(tensor[0])
            checksum += float(tensor[-1])
        del tensor
        del checkpoint
        return int(shard["tensor_bytes"]), checksum

    start = time.perf_counter()
    total_bytes = 0
    checksum = 0.0
    expected_bytes = sum(int(shard["tensor_bytes"]) for shard in manifest["shards"])
    with ProgressBar("torch_load", expected_bytes, not args.no_progress) as progress:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = [executor.submit(load_shard, shard) for shard in manifest["shards"]]
            for future in as_completed(futures):
                shard_bytes, shard_checksum = future.result()
                progress.update(shard_bytes)
                total_bytes += shard_bytes
                checksum += shard_checksum

    elapsed_s = time.perf_counter() - start
    if args.touch:
        print(f"touch_checksum={checksum}")
    return {"elapsed_s": elapsed_s, "bytes": total_bytes}


def write_exact(fd: int, block: bytes, size: int, progress: Callable[[int], None] | None = None) -> None:
    remaining = size
    view = memoryview(block)
    while remaining:
        bytes_to_write = min(remaining, len(view))
        chunk = view[:bytes_to_write]
        written_total = 0
        while chunk:
            written = os.write(fd, chunk)
            written_total += written
            chunk = chunk[written:]
        remaining -= bytes_to_write
        if progress:
            progress(written_total)


def read_exact(
    fd: int,
    block_size: int,
    size: int,
    touch: bool,
    progress: Callable[[int], None] | None = None,
) -> int:
    remaining = size
    checksum = 0
    while remaining:
        data = os.read(fd, min(remaining, block_size))
        if not data:
            raise OSError("unexpected EOF")
        remaining -= len(data)
        if progress:
            progress(len(data))
        if touch:
            checksum ^= data[0]
            checksum ^= data[-1]
    return checksum


def benchmark_raw_store(args: argparse.Namespace) -> dict[str, float]:
    element_size = element_size_from_dtype(args.dtype)
    total_bytes = args.num_params * element_size
    shard_size_bytes = max(element_size, int(args.shard_size_gib * GIB))
    shards = shard_specs(args.num_params, element_size, shard_size_bytes, "bin", args.no_shard)
    block = bytes(args.block_size_mib * MIB)

    prepare_store_dir(args.model_dir, args.overwrite)

    progress_label = "raw_store"

    def store_shard(shard: dict[str, Any], progress: Callable[[int], None]) -> dict[str, Any]:
        shard_path = args.model_dir / shard["file"]
        fd = os.open(shard_path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
        try:
            write_exact(fd, block, int(shard["tensor_bytes"]), progress)
        finally:
            os.close(fd)
        return {**shard, "file_bytes": shard_path.stat().st_size}

    start = time.perf_counter()
    with ProgressBar(progress_label, total_bytes, not args.no_progress) as progress:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = [executor.submit(store_shard, shard, progress.update) for shard in shards]
            shards = [future.result() for future in as_completed(futures)]
    shards.sort(key=lambda shard: shard["file"])

    elapsed_s = time.perf_counter() - start
    write_raw_manifest(args.model_dir, args.dtype, args.num_params, shard_size_bytes, shards, elapsed_s)
    return {"elapsed_s": elapsed_s, "bytes": total_bytes}


def benchmark_raw_load(args: argparse.Namespace) -> dict[str, float]:
    manifest = read_manifest(args.model_dir)
    if manifest.get("format") != "raw":
        raise SystemExit(f"{args.model_dir} contains a {manifest.get('format', 'torch')} checkpoint, not raw.")
    block_size = args.block_size_mib * MIB

    def load_shard(shard: dict[str, Any], progress: Callable[[int], None]) -> tuple[int, int]:
        shard_path = args.model_dir / shard["file"]
        fd = os.open(shard_path, os.O_RDONLY)
        try:
            checksum = read_exact(fd, block_size, int(shard["tensor_bytes"]), args.touch, progress)
        finally:
            os.close(fd)
        return int(shard["tensor_bytes"]), checksum

    start = time.perf_counter()
    total_bytes = 0
    checksum = 0
    expected_bytes = sum(int(shard["tensor_bytes"]) for shard in manifest["shards"])
    with ProgressBar("raw_load", expected_bytes, not args.no_progress) as progress:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = [executor.submit(load_shard, shard, progress.update) for shard in manifest["shards"]]
            for future in as_completed(futures):
                shard_bytes, shard_checksum = future.result()
                total_bytes += shard_bytes
                checksum ^= shard_checksum
    elapsed_s = time.perf_counter() - start
    if args.touch:
        print(f"touch_checksum={checksum}")
    return {"elapsed_s": elapsed_s, "bytes": total_bytes}


def usrbio_store_shard(
    args: argparse.Namespace,
    h3io: Any,
    shard: dict[str, Any],
    mount_point: str,
    progress: Callable[[int], None],
) -> dict[str, Any]:
    import multiprocessing.shared_memory as shared_memory

    block_size = args.block_size_mib * MIB
    io_depth = max(1, args.iodepth)
    shm = shared_memory.SharedMemory(size=block_size * io_depth, create=True)
    iov = h3io.make_iovec(shm, mount_point)
    shm.unlink()
    try:
        ior = h3io.make_ioring(mount_point, io_depth, False, 0)
        shard_path = args.model_dir / shard["file"]
        fd = os.open(shard_path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
        h3io.register_fd(fd)
        try:
            offset = 0
            remaining = int(shard["tensor_bytes"])
            while remaining:
                prepared = 0
                ios = []
                for slot in range(io_depth):
                    if remaining == 0:
                        break
                    size = min(block_size, remaining)
                    buf = iov[slot * block_size : slot * block_size + size]
                    io = (buf, fd, offset, size)
                    ios.append(io)
                    ior.prepare(buf, False, fd, offset, userdata=io)
                    offset += size
                    remaining -= size
                    prepared += 1
                done = ior.submit().wait(min_results=prepared, max_results=prepared)
                for result in done:
                    if result.result != result.userdata[3]:
                        raise OSError(f"short USRBIO write: {result.result} != {result.userdata[3]}")
                    progress(result.result)
            h3io.force_fsync(fd)
        finally:
            h3io.deregister_fd(fd)
            os.close(fd)
        return {**shard, "file_bytes": shard_path.stat().st_size}
    finally:
        del iov
        shm.close()


def usrbio_load_shard(
    args: argparse.Namespace,
    h3io: Any,
    shard: dict[str, Any],
    mount_point: str,
    progress: Callable[[int], None],
) -> tuple[int, int]:
    import multiprocessing.shared_memory as shared_memory

    block_size = args.block_size_mib * MIB
    io_depth = max(1, args.iodepth)
    shm = shared_memory.SharedMemory(size=block_size * io_depth, create=True)
    iov = h3io.make_iovec(shm, mount_point)
    shm.unlink()
    try:
        ior = h3io.make_ioring(mount_point, io_depth, True, 0)
        shard_path = args.model_dir / shard["file"]
        fd = os.open(shard_path, os.O_RDONLY)
        h3io.register_fd(fd)
        checksum = 0
        try:
            offset = 0
            remaining = int(shard["tensor_bytes"])
            while remaining:
                prepared = 0
                ios = []
                for slot in range(io_depth):
                    if remaining == 0:
                        break
                    size = min(block_size, remaining)
                    buf = iov[slot * block_size : slot * block_size + size]
                    io = (buf, fd, offset, size)
                    ios.append(io)
                    ior.prepare(buf, True, fd, offset, userdata=io)
                    offset += size
                    remaining -= size
                    prepared += 1
                done = ior.submit().wait(min_results=prepared, max_results=prepared)
                for result in done:
                    if result.result != result.userdata[3]:
                        raise OSError(f"short USRBIO read: {result.result} != {result.userdata[3]}")
                    progress(result.result)
                    if args.touch and result.result:
                        buf = memoryview(result.userdata[0])
                        checksum ^= buf[0]
                        checksum ^= buf[result.result - 1]
                        del buf
            return int(shard["tensor_bytes"]), checksum
        finally:
            h3io.deregister_fd(fd)
            os.close(fd)
    finally:
        del iov
        shm.close()


def benchmark_usrbio_store(args: argparse.Namespace, h3io: Any) -> dict[str, float]:
    element_size = element_size_from_dtype(args.dtype)
    total_bytes = args.num_params * element_size
    shard_size_bytes = max(element_size, int(args.shard_size_gib * GIB))
    shards = shard_specs(args.num_params, element_size, shard_size_bytes, "bin", args.no_shard)
    prepare_store_dir(args.model_dir, args.overwrite)
    mount_point = h3io.extract_mount_point(str(args.model_dir))
    if not mount_point:
        raise SystemExit(f"Could not extract 3FS mount point from {args.model_dir}")

    start = time.perf_counter()
    with ProgressBar("usrbio_store", total_bytes, not args.no_progress) as progress:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = [
                executor.submit(usrbio_store_shard, args, h3io, shard, mount_point, progress.update)
                for shard in shards
            ]
            shards = [future.result() for future in as_completed(futures)]
    shards.sort(key=lambda shard: shard["file"])

    elapsed_s = time.perf_counter() - start
    write_raw_manifest(args.model_dir, args.dtype, args.num_params, shard_size_bytes, shards, elapsed_s, fmt="usrbio")
    return {"elapsed_s": elapsed_s, "bytes": total_bytes}


def benchmark_usrbio_load(args: argparse.Namespace, h3io: Any) -> dict[str, float]:
    manifest = read_manifest(args.model_dir)
    if manifest.get("format") != "usrbio":
        raise SystemExit(f"{args.model_dir} contains a {manifest.get('format', 'torch')} checkpoint, not usrbio.")
    mount_point = h3io.extract_mount_point(str(args.model_dir))
    if not mount_point:
        raise SystemExit(f"Could not extract 3FS mount point from {args.model_dir}")

    start = time.perf_counter()
    total_bytes = 0
    checksum = 0
    expected_bytes = sum(int(shard["tensor_bytes"]) for shard in manifest["shards"])
    with ProgressBar("usrbio_load", expected_bytes, not args.no_progress) as progress:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = [
                executor.submit(usrbio_load_shard, args, h3io, shard, mount_point, progress.update)
                for shard in manifest["shards"]
            ]
            for future in as_completed(futures):
                shard_bytes, shard_checksum = future.result()
                total_bytes += shard_bytes
                checksum ^= shard_checksum
    elapsed_s = time.perf_counter() - start
    if args.touch:
        print(f"touch_checksum={checksum}")
    return {"elapsed_s": elapsed_s, "bytes": total_bytes}


def print_result(name: str, result: dict[str, float]) -> None:
    gib = result["bytes"] / GIB
    throughput = gib / result["elapsed_s"] if result["elapsed_s"] else float("inf")
    print(f"{name}: {gib:.2f} GiB in {result['elapsed_s']:.3f}s = {throughput:.2f} GiB/s")


def main() -> int:
    args = parse_args()
    if args.jobs < 1:
        raise SystemExit("--jobs must be >= 1")
    if args.iodepth < 1:
        raise SystemExit("--iodepth must be >= 1")
    if args.block_size_mib < 1:
        raise SystemExit("--block-size-mib must be >= 1")

    if args.format == "torch":
        torch = import_torch()
        if args.mode in ("store", "both"):
            print_result("torch_store", benchmark_store(args, torch))
        if args.mode in ("load", "both"):
            print_result("torch_load", benchmark_load(args, torch))
    elif args.format == "raw":
        if args.mode in ("store", "both"):
            print_result("raw_store", benchmark_raw_store(args))
        if args.mode in ("load", "both"):
            print_result("raw_load", benchmark_raw_load(args))
    elif args.format == "usrbio":
        h3io = import_usrbio()
        if args.mode in ("store", "both"):
            print_result("usrbio_store", benchmark_usrbio_store(args, h3io))
        if args.mode in ("load", "both"):
            print_result("usrbio_load", benchmark_usrbio_load(args, h3io))
    print(f"checkpoint_dir={args.model_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
