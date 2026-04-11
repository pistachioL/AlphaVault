from __future__ import annotations

import argparse
import logging
import os

from alphavault.constants import (
    DEFAULT_ALPHAVAULT_LOG_LEVEL,
    ENV_ALPHAVAULT_LOG_LEVEL,
)

LOG_LEVEL_CHOICES = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


def normalize_log_level(
    value: object,
    *,
    default: str = DEFAULT_ALPHAVAULT_LOG_LEVEL,
) -> str:
    text = str(value or "").strip().upper()
    if text in LOG_LEVEL_CHOICES:
        return text
    return str(default or DEFAULT_ALPHAVAULT_LOG_LEVEL).strip().upper()


def resolve_log_level(value: object = "") -> str:
    if str(value or "").strip():
        return normalize_log_level(value)
    return normalize_log_level(os.getenv(ENV_ALPHAVAULT_LOG_LEVEL, ""))


def add_log_level_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        default=resolve_log_level(),
        choices=[level.lower() for level in LOG_LEVEL_CHOICES],
        help=(
            "日志级别（也可用 "
            f"{ENV_ALPHAVAULT_LOG_LEVEL}；默认 {resolve_log_level().lower()}）"
        ),
    )


def configure_logging(*, level: object = "") -> str:
    resolved_level = resolve_log_level(level)
    logging.basicConfig(
        level=getattr(logging, resolved_level, logging.INFO),
        format="%(message)s",
        force=True,
    )
    return resolved_level


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


__all__ = [
    "LOG_LEVEL_CHOICES",
    "add_log_level_argument",
    "configure_logging",
    "get_logger",
    "normalize_log_level",
    "resolve_log_level",
]
