from __future__ import annotations

from pathlib import Path
import subprocess
from typing import TypedDict

PROCESS_METRICS_PS_COMMAND = ["ps", "-axo", "pid=,rss=,%cpu=,command="]
KB_PER_MB = 1024.0
BYTES_PER_MB = 1024.0 * 1024.0
DEFAULT_CGROUP_ROOT = Path("/sys/fs/cgroup")
C_GROUP_V2_LIMIT_PATH = "memory.max"
C_GROUP_V2_USED_PATH = "memory.current"
C_GROUP_V1_DIR = "memory"
C_GROUP_V1_LIMIT_PATH = "memory.limit_in_bytes"
C_GROUP_V1_USED_PATH = "memory.usage_in_bytes"


class ProcessMetricsRow(TypedDict):
    pid: int
    rss_mb: float
    cpu_percent: float
    cmdline: str


class ContainerMemoryMetrics(TypedDict):
    memory_limit_mb: float | None
    memory_used_mb: float | None
    memory_remaining_mb: float | None


def _round_mb_from_bytes(value: int) -> float:
    return round(float(value) / BYTES_PER_MB, 1)


def _read_cgroup_int(path: Path) -> int | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text or text == "max":
        return None
    try:
        value = int(text)
    except ValueError:
        return None
    return value if value >= 0 else None


def _build_container_memory_metrics(
    *, limit_bytes: int | None, used_bytes: int | None
) -> ContainerMemoryMetrics:
    limit_mb = _round_mb_from_bytes(limit_bytes) if limit_bytes is not None else None
    used_mb = _round_mb_from_bytes(used_bytes) if used_bytes is not None else None
    remaining_mb: float | None = None
    if limit_bytes is not None and used_bytes is not None:
        remaining_mb = _round_mb_from_bytes(max(limit_bytes - used_bytes, 0))
    return {
        "memory_limit_mb": limit_mb,
        "memory_used_mb": used_mb,
        "memory_remaining_mb": remaining_mb,
    }


def _parse_process_row(raw_line: str) -> ProcessMetricsRow:
    line = str(raw_line).strip()
    if not line:
        raise ValueError("empty_process_metrics_row")

    parts = line.split(None, 3)
    if len(parts) != 4:
        raise ValueError("invalid_process_metrics_row")

    pid_text, rss_kb_text, cpu_percent_text, cmdline = parts
    cmdline_text = str(cmdline).strip()
    if not cmdline_text:
        raise ValueError("missing_process_command")

    rss_kb = int(rss_kb_text)
    rss_mb = round(float(rss_kb) / KB_PER_MB, 1)
    return {
        "pid": int(pid_text),
        "rss_mb": rss_mb,
        "cpu_percent": float(cpu_percent_text),
        "cmdline": cmdline_text,
    }


def _process_sort_key(row: ProcessMetricsRow) -> tuple[float, int]:
    return (row["rss_mb"], row["pid"])


def load_process_metrics() -> list[ProcessMetricsRow]:
    result = subprocess.run(
        PROCESS_METRICS_PS_COMMAND,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(str(result.stderr or "process_metrics_ps_failed").strip())

    rows = [
        _parse_process_row(raw_line)
        for raw_line in str(result.stdout or "").splitlines()
        if str(raw_line).strip()
    ]
    rows.sort(key=_process_sort_key, reverse=True)
    return rows


def load_container_memory_metrics(
    *, cgroup_root: Path = DEFAULT_CGROUP_ROOT
) -> ContainerMemoryMetrics:
    limit_bytes = _read_cgroup_int(cgroup_root / C_GROUP_V2_LIMIT_PATH)
    used_bytes = _read_cgroup_int(cgroup_root / C_GROUP_V2_USED_PATH)
    if limit_bytes is not None or used_bytes is not None:
        return _build_container_memory_metrics(
            limit_bytes=limit_bytes,
            used_bytes=used_bytes,
        )

    v1_root = cgroup_root / C_GROUP_V1_DIR
    return _build_container_memory_metrics(
        limit_bytes=_read_cgroup_int(v1_root / C_GROUP_V1_LIMIT_PATH),
        used_bytes=_read_cgroup_int(v1_root / C_GROUP_V1_USED_PATH),
    )


__all__ = [
    "ContainerMemoryMetrics",
    "ProcessMetricsRow",
    "load_container_memory_metrics",
    "load_process_metrics",
]
