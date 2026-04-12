from __future__ import annotations

import subprocess
from typing import TypedDict

PROCESS_METRICS_PS_COMMAND = ["ps", "-axo", "pid=,rss=,%cpu=,command="]
KB_PER_MB = 1024.0
MEMORY_TEXT_UNIT = "MB"


class ProcessMetricsRow(TypedDict):
    pid: int
    rss_mb: float
    memory_text: str
    cpu_percent: float
    cmdline: str


def _format_memory_text(rss_mb: float) -> str:
    return f"{float(rss_mb):.1f} {MEMORY_TEXT_UNIT}"


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
        "memory_text": _format_memory_text(rss_mb),
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


__all__ = ["ProcessMetricsRow", "load_process_metrics"]
