"""Utility helpers for the Distributed Workflow Orchestrator."""

from __future__ import annotations

import importlib
import logging
from typing import Any, Callable


def human_readable_duration(seconds: float) -> str:
	"""Convert a duration in seconds into a short human-readable string."""

	if seconds < 0:
		raise ValueError("Duration cannot be negative")

	minutes, rem = divmod(seconds, 60)
	hours, minutes = divmod(int(minutes), 60)
	rem = round(rem, 3)

	if hours:
		return f"{hours}h {minutes}m {rem:.3f}s"
	if minutes:
		return f"{minutes}m {rem:.3f}s"
	return f"{rem:.3f}s"


def retry_backoff(attempt: int, base_delay: float = 2.0) -> float:
	"""Return exponential backoff delay for the given attempt."""

	if attempt < 0:
		raise ValueError("Attempt must be non-negative")
	return base_delay * (2**attempt)


def safe_import(callable_path: str) -> Callable[..., Any]:
	"""Import a callable from ``module:attr`` or ``module.attr`` notation."""

	if not callable_path:
		raise ValueError("callable_path must be provided")

	separator = ":" if ":" in callable_path else "."
	module_path, _, attr = callable_path.rpartition(separator)
	if not module_path or not attr:
		raise ValueError(f"Invalid callable path: {callable_path}")

	try:
		module = importlib.import_module(module_path)
	except ModuleNotFoundError as exc:
		raise ImportError(f"Module '{module_path}' not found") from exc

	try:
		target = getattr(module, attr)
	except AttributeError as exc:
		raise ImportError(f"Callable '{attr}' not found in module '{module_path}'") from exc

	if not callable(target):
		raise TypeError(f"Imported object '{callable_path}' is not callable")

	return target


def setup_logging(level: str = "INFO") -> None:
	"""Configure application-wide logging exactly once."""

	if getattr(setup_logging, "_configured", False):  # type: ignore[attr-defined]
		return

	logging.basicConfig(
		level=getattr(logging, level.upper(), logging.INFO),
		format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
	)
	setattr(setup_logging, "_configured", True)  # type: ignore[attr-defined]
