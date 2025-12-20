"""Task execution utilities for workers."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import subprocess
import time
import traceback
from multiprocessing import Process, Queue
from typing import Any, Dict, Optional

from .utils import human_readable_duration, safe_import


def _invoke_callable(func: Any, metadata: Dict[str, Any]) -> Any:
	if asyncio.iscoroutinefunction(func):
		return asyncio.run(_invoke_async(func, metadata))
	return _invoke_sync(func, metadata)


async def _invoke_async(func: Any, metadata: Dict[str, Any]) -> Any:
	return await _call_with_metadata(func, metadata)


def _invoke_sync(func: Any, metadata: Dict[str, Any]) -> Any:
	return _call_with_metadata(func, metadata)


def _call_with_metadata(func: Any, metadata: Dict[str, Any]) -> Any:
	if metadata:
		try:
			return func(**metadata)
		except TypeError:
			return func(metadata)
	return func()


def _run_callable_worker(callable_path: str, metadata: Dict[str, Any], result_queue: Queue) -> None:
	"""Execute the callable inside a separate process."""

	try:
		func = safe_import(callable_path)
		output = _invoke_callable(func, metadata)
		result_queue.put({"exit_code": 0, "stdout": json.dumps(output, default=str), "stderr": ""})
	except Exception:  # pragma: no cover - defensive path
		result_queue.put({"exit_code": 1, "stdout": "", "stderr": traceback.format_exc()})


def execute_task(task_payload: Dict[str, Any], timeout: Optional[int]) -> Dict[str, Any]:
	"""Execute a task payload and return execution metadata."""

	command = task_payload.get("command")
	callable_path = task_payload.get("callable")
	if not command and not callable_path:
		raise ValueError("Task payload must contain 'command' or 'callable'")

	started = time.monotonic()

	if command:
		try:
			completed = subprocess.run(
				command,
				shell=True,
				capture_output=True,
				text=True,
				timeout=timeout,
				check=False,
			)
			duration = human_readable_duration(time.monotonic() - started)
			status = "success" if completed.returncode == 0 else "failed"
			return {
				"status": status,
				"stdout": completed.stdout,
				"stderr": completed.stderr,
				"duration": duration,
				"exit_code": completed.returncode,
			}
		except subprocess.TimeoutExpired:
			return {
				"status": "timeout",
				"stdout": "",
				"stderr": "Command execution exceeded timeout",
				"duration": human_readable_duration(time.monotonic() - started),
				"exit_code": None,
			}

	assert callable_path  # already validated above
	result_queue: Queue = multiprocessing.Queue()
	process: Process = Process(
		target=_run_callable_worker,
		args=(callable_path, task_payload.get("metadata", {}), result_queue),
	)
	process.start()
	process.join(timeout)

	if process.is_alive():
		process.terminate()
		return {
			"status": "timeout",
			"stdout": "",
			"stderr": "Callable execution exceeded timeout",
			"duration": human_readable_duration(time.monotonic() - started),
			"exit_code": None,
		}

	process.join()
	if not result_queue.empty():
		result = result_queue.get()
		status = "success" if result["exit_code"] == 0 else "failed"
		return {
			"status": status,
			"stdout": result.get("stdout", ""),
			"stderr": result.get("stderr", ""),
			"duration": human_readable_duration(time.monotonic() - started),
			"exit_code": result.get("exit_code"),
		}

	return {
		"status": "failed",
		"stdout": "",
		"stderr": "Callable produced no output",
		"duration": human_readable_duration(time.monotonic() - started),
		"exit_code": 1,
	}
