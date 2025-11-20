"""Sample callable for DAG demo."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict


def do_b(metadata: Dict[str, Any] | None = None) -> str:
	"""Example callable used by the sample DAG."""

	metadata = metadata or {}
	timestamp = datetime.utcnow().isoformat()
	return f"Task B executed with payload={metadata} at {timestamp}"
